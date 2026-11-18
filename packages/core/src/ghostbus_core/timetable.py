"""The static timetable as history: natural keys, service days, GTFS times, versions, SCD type 2.

Pure functions, no Spark. The ``timetable_load`` Spark job uses them for the small inputs
(calendars, day origins) and is tested against the reference SCD2 implementation here.

Key rules (ADR-0002, refined by ADR-0006):

* **Route** = ``agency_id|route_short_name|route_type``. Rail (``route_type`` 2) adds
  ``route_long_name``, because Irish Rail publishes every route with the short name ``rail``.
* **Stop** = ``stop_id`` (the NaPTAN AtcoCode, stable across versions).
* **Trip instance** = route key, direction, service date, first departure, first stop, plus a
  duplicate index for trips that share all of those (two identical Luas trams, for example).
* **Times** follow the GTFS rule: seconds after "noon minus 12 h" on the service date in local
  time, so ``25:30:00`` is 01:30 the next morning and the origin moves on clock-change days.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

#: All NTA agencies publish ``Europe/London``; Dublin has identical offsets and is what we mean.
LOCAL_TZ = ZoneInfo("Europe/Dublin")
ACCEPTED_AGENCY_TIMEZONES = frozenset({"Europe/Dublin", "Europe/London"})
RAIL_ROUTE_TYPE = "2"
#: Which version's schedule defines a service day: the one live at this local time on that day.
SCHEDULE_CUTOFF_LOCAL = time(4, 0)

_WEEKDAYS = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")


# --- keys -----------------------------------------------------------------------------------------


def route_key(
    agency_id: str, route_short_name: str, route_type: str, route_long_name: str = ""
) -> str:
    parts = [agency_id.strip(), route_short_name.strip(), route_type.strip()]
    if route_type.strip() == RAIL_ROUTE_TYPE:
        parts.append(route_long_name.strip())
    if not parts[0] or not parts[2]:
        raise ValueError("agency_id and route_type are required for a route key")
    return "|".join(parts)


def trip_instance_key(
    route: str,
    direction_id: str,
    service_date: date,
    first_departure_s: int,
    first_stop_id: str,
    duplicate_index: int = 0,
) -> str:
    base = (
        f"{route}|{direction_id}|{service_date:%Y%m%d}|"
        f"{format_gtfs_time(first_departure_s)}|{first_stop_id}"
    )
    return base if duplicate_index == 0 else f"{base}|{duplicate_index}"


# --- GTFS dates and times -------------------------------------------------------------------------


def parse_gtfs_date(s: str) -> date:
    s = s.strip()
    if len(s) != 8 or not s.isdigit():
        raise ValueError(f"not a GTFS date: {s!r}")
    return date(int(s[:4]), int(s[4:6]), int(s[6:]))


def parse_gtfs_time(s: str) -> int:
    """``"25:30:00"`` -> 91800. Hours may exceed 23 (trips after midnight)."""
    parts = s.strip().split(":")
    if len(parts) != 3 or not all(p.isdigit() for p in parts):
        raise ValueError(f"not a GTFS time: {s!r}")
    h, m, sec = (int(p) for p in parts)
    if m > 59 or sec > 59:
        raise ValueError(f"not a GTFS time: {s!r}")
    return h * 3600 + m * 60 + sec


def format_gtfs_time(seconds: int) -> str:
    if seconds < 0:
        raise ValueError("GTFS times are never negative")
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def service_day_origin(service_date: date, tz: ZoneInfo = LOCAL_TZ) -> datetime:
    """The instant GTFS times on ``service_date`` count from: local noon minus 12 hours (UTC).

    On most days that is local midnight. On the spring clock change it is 23:00 the evening
    before, and on the autumn change 01:00, which is exactly what the GTFS reference requires.
    """
    noon = datetime.combine(service_date, time(12), tzinfo=tz)
    return noon.astimezone(UTC) - timedelta(hours=12)


def gtfs_time_to_utc(service_date: date, seconds: int, tz: ZoneInfo = LOCAL_TZ) -> datetime:
    return service_day_origin(service_date, tz) + timedelta(seconds=seconds)


# --- calendars ------------------------------------------------------------------------------------


def service_dates(
    calendar: Iterable[Mapping[str, str]], calendar_dates: Iterable[Mapping[str, str]]
) -> dict[str, set[date]]:
    """Every date each ``service_id`` runs: weekly patterns, then exceptions.

    ``calendar_dates`` type 1 adds a date (even outside the calendar range or for a service with
    no calendar row); type 2 removes one. Removal is applied after all additions.
    """
    out: dict[str, set[date]] = defaultdict(set)
    for row in calendar:
        sid = row["service_id"]
        out[sid]  # a service with no running days still exists
        start, end = parse_gtfs_date(row["start_date"]), parse_gtfs_date(row["end_date"])
        flags = [row[d].strip() == "1" for d in _WEEKDAYS]
        d = start
        while d <= end:
            if flags[d.weekday()]:
                out[sid].add(d)
            d += timedelta(days=1)
    removals: list[tuple[str, date]] = []
    for row in calendar_dates:
        sid, d, kind = row["service_id"], parse_gtfs_date(row["date"]), row["exception_type"]
        if kind.strip() == "1":
            out[sid].add(d)
        elif kind.strip() == "2":
            removals.append((sid, d))
        else:
            raise ValueError(f"unknown exception_type {kind!r}")
    for sid, d in removals:
        out[sid].discard(d)
    return dict(out)


# --- feed versions --------------------------------------------------------------------------------


@dataclass(frozen=True)
class FeedVersion:
    feed_version: str
    sha256: str
    #: When it became the live version: the publish time (``Last-Modified``) if known.
    valid_from: datetime
    feed_start_date: date | None = None
    feed_end_date: date | None = None


def version_key(label: str, sha256: str) -> str:
    """The unique key of one archived timetable zip: NTA's ``feed_info.feed_version`` label plus
    the start of the zip's SHA-256.

    The label alone is not unique. NTA re-uploads every night, and two uploads can carry the same
    label with different contents (on 14 and 15 November 2026 the second one's calendar started a
    day later). Keyed by label, the second overwrote the first's per-version tables and 15
    November lost 5,182 of its 12,185 trips. Found by the first cloud load (all four versions).
    """
    return f"{label}-{sha256[:8]}"


def order_versions(versions: Iterable[FeedVersion]) -> list[FeedVersion]:
    """Oldest first. Ties (same publish second) break on the hash so the order is total."""
    return sorted(versions, key=lambda v: (v.valid_from, v.sha256))


def version_live_at(versions: Sequence[FeedVersion], at: datetime) -> FeedVersion | None:
    """The version realtime ids published at ``at`` belong to: the newest with valid_from <= at."""
    live = None
    for v in order_versions(versions):
        if v.valid_from <= at:
            live = v
    return live


#: How long after a timetable publish the realtime feed may still use the previous version's
#: trip ids. Measured: 76 minutes after the 15 Nov 2026 republish (measurement 02, transition).
ID_SWITCH_GRACE = timedelta(hours=3)


def versions_for_realtime(
    versions: Sequence[FeedVersion], at: datetime, grace: timedelta = ID_SWITCH_GRACE
) -> list[FeedVersion]:
    """Versions a realtime ``trip_id`` fetched at ``at`` may belong to, most likely first.

    The live version first. Within ``grace`` of its publish, also the one before it: NTA's
    realtime feed can keep the previous zip's ids for a while after a new zip is published, and
    ids are regenerated per zip, so an id found only in the previous version belongs to it.
    """
    ordered = order_versions(versions)
    live = [i for i, v in enumerate(ordered) if v.valid_from <= at]
    if not live:
        return []
    i = live[-1]
    out = [ordered[i]]
    if i > 0 and at < ordered[i].valid_from + grace:
        out.append(ordered[i - 1])
    return out


def schedule_version_for(versions: Sequence[FeedVersion], service_date: date) -> FeedVersion | None:
    """The version whose timetable defines ``service_date``: the one live at 04:00 local that day.

    A version published mid-day does not rewrite a day already under way; it takes over from the
    next service day. Realtime matching still uses :func:`version_live_at`, and the natural trip
    key bridges the two when they differ.
    """
    cutoff = datetime.combine(service_date, SCHEDULE_CUTOFF_LOCAL, tzinfo=LOCAL_TZ)
    return version_live_at(versions, cutoff.astimezone(UTC))


# --- SCD type 2 -----------------------------------------------------------------------------------


def row_hash(attrs: Mapping[str, object]) -> str:
    """Stable hash of the tracked attributes (key order and whitespace do not matter)."""
    canon = json.dumps(
        {k: (v.strip() if isinstance(v, str) else v) for k, v in attrs.items()},
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Scd2Row:
    key: str
    attrs: Mapping[str, object]
    row_hash: str
    feed_version: str
    valid_from: datetime
    valid_to: datetime | None = None

    @property
    def is_current(self) -> bool:
        return self.valid_to is None


def scd2_apply(
    history: Sequence[Scd2Row],
    incoming: Mapping[str, Mapping[str, object]],
    version: FeedVersion,
) -> list[Scd2Row]:
    """Reference SCD2: apply one version's snapshot of a dimension to its full history.

    * key new, or attributes changed -> close the current row at ``version.valid_from``, open one
    * key absent from the snapshot   -> close the current row (the route or stop was withdrawn)
    * unchanged                      -> nothing
    Closed rows are never modified again or deleted. Applying the same version twice is a no-op.
    """
    current = {r.key: r for r in history if r.is_current}
    if any(r.valid_from > version.valid_from for r in current.values()):
        raise ValueError(f"version {version.feed_version} is older than the loaded history")
    out = [r for r in history if not r.is_current]
    at = version.valid_from
    for key, row in current.items():
        if key not in incoming or row.row_hash != row_hash(incoming[key]):
            out.append(replace(row, valid_to=at))
        else:
            out.append(row)
    for key, attrs in incoming.items():
        h = row_hash(attrs)
        if key not in current or current[key].row_hash != h:
            out.append(Scd2Row(key, dict(attrs), h, version.feed_version, at))
    return sorted(out, key=lambda r: (r.key, r.valid_from))


def resolve(history: Sequence[Scd2Row], key: str, at: datetime) -> Scd2Row | None:
    """The definition of ``key`` that was in force at ``at`` (point-in-time lookup)."""
    for r in history:
        if r.key == key and r.valid_from <= at and (r.valid_to is None or at < r.valid_to):
            return r
    return None
