"""'Will my bus come?': the next departures at a stop, live status and a probability (ADR-0012).

Pure, no AWS. Three parts:

- **The stop index**: the nightly job turns the timetable into 256 shards of departures by stop
  (:func:`index_docs`); the API reads one shard per request (:func:`read_shard`).
- **Live**: the newest TripUpdates and Vehicles snapshots from the archive become a :class:`Live`
  view, matched to scheduled trips by trip id or, after a timetable republish regenerated the
  ids, by route, direction, date and start time.
- **The answer**: :func:`departures_doc` picks the departures in the window, works out each one's
  status and expected time, and its probability from the route's published history.
"""

from __future__ import annotations

import gzip
import json
import zlib
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from functools import cached_property
from typing import Any

from google.transit import gtfs_realtime_pb2 as pb

from ghostbus_core.export import ATTRIBUTION, day_type
from ghostbus_core.outcomes import DEFAULT
from ghostbus_core.timetable import LOCAL_TZ

SHARDS = 256
SCHEMA_VERSION = "v1"
MODEL = "heuristic-v1"
#: Live data older than this is labelled stale; older than MAX_LIVE_AGE_S it is not used.
FRESH_S = 300
MAX_LIVE_AGE_S = 900
#: How far back a scheduled departure may be and still be shown (a late bus is still coming).
LOOKBACK_S = 30 * 60
#: A departure whose expected time is this far in the past has gone.
GONE_S = 60
#: The prior's weight, in trips, when shrinking an hour's history toward the route's overall rate.
PRIOR_TRIPS = 20
#: An early cancellation is announced at least this long before departure (Params.cancel_lead_min).
CANCEL_LEAD_S = DEFAULT.cancel_lead_min * 60
DIGITS = 2

STATUSES = ("cancelled", "skips_stop", "tracked", "assigned", "scheduled", "no_live_data")


def shard_of(stop_code: str) -> int:
    """The shard a stop's departures live in. ``zlib.crc32`` is stable across runs and machines
    (unlike ``hash()``), so the job that writes and the API that reads always agree."""
    return zlib.crc32(stop_code.encode("utf-8")) % SHARDS


def shard_key(build: str, shard: int) -> str:
    return f"builds/{build}/shard={shard:03d}.json.gz"


POINTER_KEY = "current.json"


# --- the stop index -------------------------------------------------------------------------------


@dataclass(frozen=True)
class Trip:
    """One scheduled bus trip on one service day, as the live feed identifies it."""

    trip_id: str
    route_id: str
    direction_id: str
    start_date: str  # YYYYMMDD, the service day
    start_time: str  # HH:MM:SS, GTFS time of the first departure (may exceed 24:00)
    start_at: int  # the first departure, epoch seconds
    agency: str
    route: str  # short name, e.g. "46A"
    headsign: str
    slug: str  # the route's file in data/v1/routes/

    def row(self) -> list[Any]:
        return [
            self.trip_id,
            self.route_id,
            self.direction_id,
            self.start_date,
            self.start_time,
            self.start_at,
            self.agency,
            self.route,
            self.headsign,
            self.slug,
        ]

    @classmethod
    def from_row(cls, r: list[Any]) -> Trip:
        return cls(r[0], r[1], r[2], r[3], r[4], int(r[5]), r[6], r[7], r[8], r[9])


@dataclass(frozen=True)
class Departure:
    stop_code: str
    at: int  # scheduled departure at this stop, epoch seconds
    stop_sequence: int
    trip: Trip


@dataclass(frozen=True)
class StopInfo:
    code: str
    name: str
    lat: float
    lon: float


def _gzip(doc: Mapping[str, Any]) -> bytes:
    raw = json.dumps(doc, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return gzip.compress(raw, compresslevel=6, mtime=0)  # mtime=0: same data, same bytes


def index_docs(
    stops: Iterable[StopInfo],
    departures: Iterable[Departure],
    build: str,
    feed_versions: Iterable[str],
    valid_until: int | None = None,
) -> dict[str, bytes]:
    """Every shard (gzipped JSON) plus the pointer, keyed by path under ``api/v1/``.

    ``valid_until`` is when the first service day the index does not cover begins: after it the
    API refuses to answer rather than show a nearly empty board (the last departure is no guide:
    one overnight coach can run to 07:00 the morning after).

    Every shard is written, even an empty one, so a missing file always means a broken build and
    never "no stops here". The pointer is the last key in the returned order: write it last.
    """
    info = {s.code: s for s in stops}
    by_stop: dict[str, list[Departure]] = defaultdict(list)
    for d in departures:
        if d.stop_code in info:
            by_stop[d.stop_code].append(d)
    out: dict[str, bytes] = {}
    first: int | None = None
    last: int | None = None
    total = 0
    by_shard: dict[int, list[str]] = defaultdict(list)
    for c in by_stop:
        by_shard[shard_of(c)].append(c)
    for shard in range(SHARDS):
        codes = sorted(by_shard[shard])
        trips = sorted({d.trip for c in codes for d in by_stop[c]}, key=lambda t: t.row())
        ref = {t: i for i, t in enumerate(trips)}
        stops_out = {}
        for c in codes:
            deps = sorted(by_stop[c], key=lambda d: (d.at, d.stop_sequence, d.trip.row()))
            total += len(deps)
            first = deps[0].at if first is None else min(first, deps[0].at)
            last = deps[-1].at if last is None else max(last, deps[-1].at)
            s = info[c]
            stops_out[c] = {
                "name": s.name,
                "lat": round(s.lat, 5),
                "lon": round(s.lon, 5),
                "departures": [[d.at, d.stop_sequence, ref[d.trip]] for d in deps],
            }
        out[shard_key(build, shard)] = _gzip(
            {
                "schema": f"ghostbus.stop_index.{SCHEMA_VERSION}",
                "build": build,
                "shard": shard,
                "trips": [t.row() for t in trips],
                "stops": stops_out,
            }
        )
    pointer = {
        "schema": f"ghostbus.stop_index_pointer.{SCHEMA_VERSION}",
        "build": build,
        "shards": SHARDS,
        "feed_versions": sorted(set(feed_versions)),
        "stops": len(by_stop),
        "departures": total,
        "first_departure": _iso(first) if first is not None else None,
        "last_departure": _iso(last) if last is not None else None,
        "valid_until": _iso(valid_until) if valid_until is not None else None,
    }
    out[POINTER_KEY] = json.dumps(pointer, sort_keys=True, indent=1).encode()
    return out


def read_shard(data: bytes) -> tuple[dict[str, StopInfo], dict[str, list[Departure]]]:
    doc = json.loads(gzip.decompress(data))
    trips = [Trip.from_row(r) for r in doc["trips"]]
    info: dict[str, StopInfo] = {}
    deps: dict[str, list[Departure]] = {}
    for code, s in doc["stops"].items():
        info[code] = StopInfo(code, s["name"], s["lat"], s["lon"])
        deps[code] = [Departure(code, at, seq, trips[i]) for at, seq, i in s["departures"]]
    return info, deps


# --- live -----------------------------------------------------------------------------------------


@dataclass(frozen=True)
class StopUpdate:
    stop_sequence: int | None
    stop_id: str | None
    delay: int | None  # seconds late (negative: early)
    time: int | None  # absolute prediction, epoch seconds
    skipped: bool


@dataclass(frozen=True)
class LiveTrip:
    trip_id: str | None
    route_id: str | None
    direction_id: str | None
    start_date: str | None
    start_time: str | None
    cancelled: bool
    vehicle_id: str | None
    #: The protobuf it came from. Stop updates are parsed from it only when a stop needs this
    #: trip: a snapshot has ~3,500 trips and tens of thousands of updates, a stop needs ~10 trips.
    source: pb.TripUpdate | None = field(default=None, compare=False, repr=False)

    @cached_property
    def updates(self) -> tuple[StopUpdate, ...]:
        if self.source is None:
            return ()
        skipped = pb.TripUpdate.StopTimeUpdate.SKIPPED
        out = []
        for u in self.source.stop_time_update:
            ev = u.departure if u.HasField("departure") else u.arrival
            has = u.HasField("departure") or u.HasField("arrival")
            out.append(
                StopUpdate(
                    u.stop_sequence if u.HasField("stop_sequence") else None,
                    u.stop_id if u.HasField("stop_id") else None,
                    ev.delay if has and ev.HasField("delay") else None,
                    ev.time if has and ev.HasField("time") else None,
                    u.schedule_relationship == skipped,
                )
            )
        return tuple(out)


@dataclass(frozen=True)
class Position:
    vehicle_id: str | None
    timestamp: int | None
    current_stop_sequence: int | None


def _natural(
    route_id: str | None, direction_id: str | None, start_date: str | None, start_time: str | None
) -> tuple[str, str, str, str] | None:
    if not (route_id and direction_id is not None and start_date and start_time):
        return None
    return (route_id, direction_id, start_date, start_time)


def _desc(
    t: pb.TripDescriptor,
) -> tuple[str | None, str | None, str | None, str | None, str | None]:
    return (
        t.trip_id if t.HasField("trip_id") else None,
        t.route_id if t.HasField("route_id") else None,
        str(t.direction_id) if t.HasField("direction_id") else None,
        t.start_date if t.HasField("start_date") else None,
        t.start_time if t.HasField("start_time") else None,
    )


def read_trip_updates(msg: pb.FeedMessage) -> list[LiveTrip]:
    out = []
    for e in msg.entity:
        if not e.HasField("trip_update"):
            continue
        tu = e.trip_update
        trip_id, route_id, direction_id, start_date, start_time = _desc(tu.trip)
        out.append(
            LiveTrip(
                trip_id,
                route_id,
                direction_id,
                start_date,
                start_time,
                tu.trip.schedule_relationship == pb.TripDescriptor.CANCELED,
                tu.vehicle.id if tu.HasField("vehicle") and tu.vehicle.HasField("id") else None,
                tu,
            )
        )
    return out


@dataclass
class Live:
    """What the newest snapshots say, indexed for matching. ``*_at`` are feed header times."""

    trip_updates_at: int | None = None
    vehicles_at: int | None = None
    trips_by_id: dict[tuple[str, str], LiveTrip] = field(default_factory=dict)
    trips_by_key: dict[tuple[str, str, str, str], LiveTrip] = field(default_factory=dict)
    positions_by_id: dict[tuple[str, str], Position] = field(default_factory=dict)
    positions_by_key: dict[tuple[str, str, str, str], Position] = field(default_factory=dict)

    def trip(self, t: Trip) -> tuple[LiveTrip | None, str | None]:
        """The live trip for a scheduled one, and how it was matched."""
        hit = self.trips_by_id.get((t.trip_id, t.start_date))
        if hit is not None:
            return hit, "trip_id"
        hit = self.trips_by_key.get((t.route_id, t.direction_id, t.start_date, t.start_time))
        return (hit, "route_and_start_time") if hit is not None else (None, None)

    def position(self, t: Trip) -> Position | None:
        return self.positions_by_id.get((t.trip_id, t.start_date)) or self.positions_by_key.get(
            (t.route_id, t.direction_id, t.start_date, t.start_time)
        )


def live_view(trip_updates: pb.FeedMessage | None, vehicles: pb.FeedMessage | None) -> Live:
    live = Live()
    if trip_updates is not None:
        if trip_updates.header.HasField("timestamp"):
            live.trip_updates_at = trip_updates.header.timestamp
        for lt in read_trip_updates(trip_updates):
            if lt.trip_id and lt.start_date:
                live.trips_by_id[(lt.trip_id, lt.start_date)] = lt
            key = _natural(lt.route_id, lt.direction_id, lt.start_date, lt.start_time)
            if key:
                live.trips_by_key[key] = lt
    if vehicles is not None:
        if vehicles.header.HasField("timestamp"):
            live.vehicles_at = vehicles.header.timestamp
        for e in vehicles.entity:
            if not (e.HasField("vehicle") and e.vehicle.HasField("trip")):
                continue
            v = e.vehicle
            if not v.HasField("position"):
                continue
            trip_id, route_id, direction_id, start_date, start_time = _desc(v.trip)
            pos = Position(
                v.vehicle.id if v.HasField("vehicle") and v.vehicle.HasField("id") else None,
                v.timestamp if v.HasField("timestamp") else None,
                v.current_stop_sequence if v.HasField("current_stop_sequence") else None,
            )
            if trip_id and start_date:
                live.positions_by_id[(trip_id, start_date)] = pos
            key = _natural(route_id, direction_id, start_date, start_time)
            if key:
                live.positions_by_key[key] = pos
    return live


def expected_at(d: Departure, lt: LiveTrip) -> tuple[int | None, int | None, bool]:
    """(expected departure, delay in seconds, skips this stop) from the trip's stop updates.

    GTFS-Realtime propagation: the prediction for this stop if there is one, else the delay at
    the last earlier stop that has one. A later stop's prediction says nothing about this one.
    """
    before: StopUpdate | None = None
    for u in lt.updates:
        if u.stop_sequence is None:
            continue
        if u.stop_sequence == d.stop_sequence:
            if u.skipped:
                return None, None, True
            if u.time is not None:
                return u.time, u.time - d.at, False
            if u.delay is not None:
                return d.at + u.delay, u.delay, False
        elif (
            u.stop_sequence < d.stop_sequence
            and u.delay is not None
            and not u.skipped
            and (before is None or (before.stop_sequence or 0) < u.stop_sequence)
        ):
            before = u
    if before is not None and before.delay is not None:
        return d.at + before.delay, before.delay, False
    return None, None, False


# --- history and the probability ------------------------------------------------------------------


@dataclass(frozen=True)
class Tally:
    n: int = 0
    ran: int = 0
    ghost: int = 0
    phantom: int = 0
    early: int = 0  # cancelled early


@dataclass(frozen=True)
class RouteHistory:
    total: Tally
    cells: Mapping[tuple[str, int], Tally]


def _k(row: Mapping[str, Any], name: str) -> int:
    r = row.get(name)
    return int(r["k"]) if isinstance(r, Mapping) else 0


def _tally(row: Mapping[str, Any]) -> Tally:
    n = int(row.get("trips_in_rates") or 0)
    if "ghost" in row and "phantom" in row:
        ghost, phantom = _k(row, "ghost"), _k(row, "phantom")
    else:  # files exported before the split: count every untracked trip as phantom (cautious)
        ghost, phantom = 0, _k(row, "untracked")
    return Tally(n, _k(row, "ran"), ghost, phantom, _k(row, "cancelled_early"))


def history_from_route_doc(doc: Mapping[str, Any]) -> RouteHistory:
    """A route's published history (``data/v1/routes/{slug}.json``)."""
    cells = {(c["day_type"], int(c["hour"])): _tally(c) for c in doc.get("by_hour", [])}
    return RouteHistory(_tally(doc.get("total") or {}), cells)


def _conditioned(t: Tally, status: str, near: bool) -> tuple[int, int]:
    """(ran, trips) among the outcomes the live status still allows."""
    out = 0
    if status in ("tracked", "assigned"):
        out += t.ghost + t.early
        if status == "tracked":
            out += t.phantom
    elif status == "scheduled" and near:
        out += t.early
    return t.ran, max(t.n - out, 0)


def probability(
    history: RouteHistory | None, d: Departure, status: str, now: int, live_fresh: bool
) -> tuple[float | None, dict[str, Any]]:
    """The chance the trip is RAN, given its live status (table in ADR-0012)."""
    if status in ("cancelled", "skips_stop"):
        return 0.0, {"reason": status.replace("_", " ")}
    if history is None or history.total.n == 0:
        return None, {"reason": "no published history for this route yet"}
    near = live_fresh and d.at - now < CANCEL_LEAD_S
    k_route, n_route = _conditioned(history.total, status, near)
    if n_route == 0:
        return None, {"reason": "no comparable trips in this route's history"}
    start = datetime.fromtimestamp(d.trip.start_at, UTC).astimezone(LOCAL_TZ)
    cell_key = (day_type(date.fromisoformat(_iso_date(d.trip.start_date))), start.hour)
    k_cell, n_cell = _conditioned(history.cells.get(cell_key, Tally()), status, near)
    prior = k_route / n_route
    p = (k_cell + PRIOR_TRIPS * prior) / (n_cell + PRIOR_TRIPS)
    return round(p, DIGITS), {
        "day_type": cell_key[0],
        "hour": cell_key[1],
        "trips_this_hour": n_cell,
        "trips_on_route": n_route,
    }


# --- the answer -----------------------------------------------------------------------------------


def _iso(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _iso_date(yyyymmdd: str) -> str:
    return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:]}"


def live_status(live: Live | None, now: int) -> dict[str, Any]:
    at = live.trip_updates_at if live else None
    if at is None:
        return {
            "status": "unavailable",
            "trip_updates_at": None,
            "vehicles_at": None,
            "age_s": None,
        }
    age = max(now - at, 0)
    status = "fresh" if age <= FRESH_S else "stale" if age <= MAX_LIVE_AGE_S else "unavailable"
    vat = live.vehicles_at if live else None
    return {
        "status": status,
        "trip_updates_at": _iso(at),
        "vehicles_at": _iso(vat) if vat is not None else None,
        "age_s": age,
    }


def departures_doc(
    stop: StopInfo,
    departures: Iterable[Departure],
    live: Live | None,
    history: Mapping[str, RouteHistory | None],
    now: int,
    minutes: int = 60,
    limit: int = 10,
) -> dict[str, Any]:
    """The response for ``GET /v1/stops/{code}/departures``."""
    meta = live_status(live, now)
    usable = live if meta["status"] in ("fresh", "stale") else None
    fresh = meta["status"] == "fresh"
    horizon = now + minutes * 60
    rows: list[tuple[int, int, str, dict[str, Any]]] = []
    for d in departures:
        if not (now - LOOKBACK_S <= d.at <= horizon):
            continue
        expected = delay = None
        matched_by = None
        if usable is None:
            status = "no_live_data"
        else:
            lt, matched_by = usable.trip(d.trip)
            pos = usable.position(d.trip)
            if (
                pos
                and pos.current_stop_sequence is not None
                and pos.current_stop_sequence > (d.stop_sequence)
            ):
                continue  # the bus is already past this stop
            skips = False
            if lt is not None:
                expected, delay, skips = expected_at(d, lt)
            if lt is not None and lt.cancelled:
                status = "cancelled"
            elif skips:
                status = "skips_stop"
            elif pos is not None:
                status = "tracked"
            elif lt is not None and lt.vehicle_id:
                status = "assigned"
            else:
                status = "scheduled"
        when = expected if expected is not None else d.at
        if when < now - GONE_S or when > horizon:
            continue
        if expected is None and d.at < now - GONE_S:
            continue  # no prediction and the scheduled time has passed: cannot say it is coming
        p, basis = probability(history.get(d.trip.slug), d, status, now, fresh)
        rows.append(
            (
                when,
                d.at,
                d.trip.route,
                {
                    "route": d.trip.route,
                    "agency": d.trip.agency,
                    "headsign": d.trip.headsign,
                    "scheduled": _iso(d.at),
                    "expected": _iso(expected) if expected is not None else None,
                    "delay_s": delay,
                    "status": status,
                    "probability": p,
                    "basis": basis,
                    "matched_by": matched_by,
                    "trip_id": d.trip.trip_id,
                    "route_slug": d.trip.slug,
                },
            )
        )
    rows.sort(key=lambda r: (r[0], r[1], r[2]))
    return {
        "schema": f"ghostbus.departures.{SCHEMA_VERSION}",
        "attribution": ATTRIBUTION,
        "stop": {"code": stop.code, "name": stop.name, "lat": stop.lat, "lon": stop.lon},
        "generated_at": _iso(now),
        "window_minutes": minutes,
        "live": meta,
        "model": MODEL,
        "departures": [r[3] for r in rows[:limit]],
    }
