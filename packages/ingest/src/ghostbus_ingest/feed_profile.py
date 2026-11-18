"""Profile what the collector has archived: uptime, sizes, lag, and what the feeds contain.

Reads only the raw archive (heartbeats, snapshots, the latest static version), so it can be re-run
at any time without touching the API. Prints Markdown for ``docs/measurements/01-feed-profile.md``.

Usage::

    export ARCHIVE_URI=file:///C:/Users/<you>/ghostbus-archive
    uv run python -m ghostbus_ingest.feed_profile [--since 2026-11-15T19:30Z] [--sample 60]
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import tempfile
from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from ghostbus_core.archive_layout import STATIC_FILENAME, STATIC_STATE_KEY, static_prefix
from ghostbus_core.feeds import Feed, feed_for_slot, minute_slot
from ghostbus_core.gtfsr import parse_feed
from ghostbus_core.static_feed import read_csv_member
from ghostbus_ingest.archive import Archive, archive_from_env, decompress

# ---------------------------------------------------------------------------------------------


def pct(values: Sequence[float], q: float) -> float:
    if not values:
        return float("nan")
    s = sorted(values)
    return s[min(len(s) - 1, round(q * (len(s) - 1)))]


def fmt_ts(ms: float) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=UTC).strftime("%Y-%m-%d %H:%M UTC")


def table(headers: Sequence[str], rows: Iterable[Sequence[object]]) -> str:
    out = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def n(x: float, digits: int = 0) -> str:
    return f"{x:,.{digits}f}"


# ---------------------------------------------------------------------------------------------


@dataclass
class Static:
    feed_version: str
    route_agency: dict[str, str]
    agency_name: dict[str, str]
    trip_ids: set[str]


def load_static(archive: Archive) -> Static | None:
    raw = archive.get_bytes(STATIC_STATE_KEY)
    if not raw:
        return None
    state = json.loads(raw)
    data = archive.get_bytes(static_prefix(state["sha256"]) + STATIC_FILENAME)
    if data is None:
        return None
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / STATIC_FILENAME
        path.write_bytes(data)
        routes = read_csv_member(path, "routes.txt")
        agencies = read_csv_member(path, "agency.txt")
        trips = read_csv_member(path, "trips.txt")
    return Static(
        feed_version=str(state.get("feed_version")),
        route_agency={r["route_id"]: r["agency_id"] for r in routes},
        agency_name={a["agency_id"]: a["agency_name"] for a in agencies},
        trip_ids={t["trip_id"] for t in trips},
    )


@dataclass
class FeedStats:
    beats: list[dict[str, object]] = field(default_factory=list)
    compressed: list[int] = field(default_factory=list)


def load_heartbeats(archive: Archive, since_ms: int) -> dict[Feed, FeedStats]:
    out: dict[Feed, FeedStats] = defaultdict(FeedStats)
    for key in archive.list_keys("raw/feed=heartbeat/"):
        raw = archive.get_bytes(key)
        if not raw:
            continue
        hb = json.loads(raw)
        if int(hb["fetched_at_ms"]) < since_ms:
            continue
        out[Feed(hb["feed"])].beats.append(hb)
    return out


def _i(v: object) -> int:
    return int(v) if isinstance(v, int | float | str) else 0


def uptime_section(stats: dict[Feed, FeedStats], archive: Archive) -> str:
    all_beats = [hb for s in stats.values() for hb in s.beats]
    first = min(_i(hb["fetched_at_ms"]) for hb in all_beats)
    last = max(_i(hb["fetched_at_ms"]) for hb in all_beats)
    slots = range(minute_slot(first / 1000), minute_slot(last / 1000) + 1)
    expected = Counter(feed_for_slot(s) for s in slots)
    rows = []
    for feed in Feed:
        beats = stats.get(feed, FeedStats()).beats
        statuses = Counter(str(hb["status"]) for hb in beats)
        collected = sum(1 for hb in beats if hb.get("collected"))
        elapsed = [float(_i(hb["elapsed_ms"])) for hb in beats if hb["status"] == "ok"]
        rows.append(
            [
                feed.value,
                expected[feed],
                len(beats),
                collected,
                f"{100 * collected / expected[feed]:.1f}%" if expected[feed] else "-",
                statuses.get("throttled", 0),
                ", ".join(f"{k} {v}" for k, v in statuses.items() if k != "ok") or "none",
                n(pct(elapsed, 0.5)),
                n(pct(elapsed, 0.95)),
            ]
        )
    head = (
        f"Window: {fmt_ts(first)} to {fmt_ts(last)} "
        f"({(last - first) / 3_600_000:.1f} h, {len(slots)} minute slots). "
        f"Archive: `{archive.uri}`."
    )
    return (
        head
        + "\n\n"
        + table(
            [
                "Feed",
                "Scheduled calls",
                "Calls made",
                "Collected",
                "Success",
                "429s",
                "Failures",
                "p50 ms",
                "p95 ms",
            ],
            rows,
        )
    )


def snapshots(
    archive: Archive, stats: FeedStats, sample: int
) -> list[tuple[dict[str, object], bytes, int]]:
    ok = [hb for hb in stats.beats if hb.get("collected") and hb.get("archive_key")]
    if sample and len(ok) > sample:
        step = len(ok) / sample
        ok = [ok[int(i * step)] for i in range(sample)]
    out = []
    for hb in ok:
        packed = archive.get_bytes(str(hb["archive_key"]))
        if packed is not None:
            out.append((hb, decompress(packed), len(packed)))
    return out


def size_section(per_feed: dict[Feed, list[tuple[dict[str, object], bytes, int]]]) -> str:
    rows = []
    daily_mb = 0.0
    for feed, snaps in per_feed.items():
        if not snaps:
            continue
        raw = [len(r) for _, r, _ in snaps]
        packed = [c for _, _, c in snaps]
        entities = [_i(hb["entity_count"]) for hb, _, _ in snaps]
        lag = [
            _i(hb["fetched_at_ms"]) / 1000 - _i(hb["feed_timestamp"])
            for hb, _, _ in snaps
            if hb.get("feed_timestamp") is not None
        ]
        calls_per_day = sum(1 for s in range(1440) if feed_for_slot(s) is feed)
        daily_mb += statistics.mean(packed) * calls_per_day / 1e6
        rows.append(
            [
                feed.value,
                len(snaps),
                n(statistics.mean(raw) / 1000),
                n(statistics.mean(packed) / 1000),
                f"{statistics.mean(raw) / statistics.mean(packed):.1f}x",
                f"{min(entities):,} / {n(statistics.mean(entities))} / {max(entities):,}",
                f"{n(pct(lag, 0.5))} / {n(pct(lag, 0.95))} / {n(max(lag) if lag else 0)}",
            ]
        )
    return (
        table(
            [
                "Feed",
                "Snapshots read",
                "Raw KB (mean)",
                "zstd KB (mean)",
                "Ratio",
                "Entities min / mean / max",
                "Header lag s p50 / p95 / max",
            ],
            rows,
        )
        + f"\n\nArchive growth at this schedule: about **{daily_mb:,.0f} MB per day** "
        f"({daily_mb * 30 / 1000:,.1f} GB per 30 days)."
    )


def trip_updates_section(
    snaps: list[tuple[dict[str, object], bytes, int]], static: Static | None
) -> str:
    rel_totals: Counter[str] = Counter()
    with_vehicle = with_trip_id = total = stus = 0
    uses_delay = uses_time = 0
    unmatched_trip = unmatched_route = matchable = 0
    per_agency: Counter[str] = Counter()
    per_agency_vehicle: Counter[str] = Counter()
    for _, raw, _ in snaps:
        msg = parse_feed(raw)
        for e in msg.entity:
            tu = e.trip_update
            total += 1
            rel = tu.trip.DESCRIPTOR.fields_by_name["schedule_relationship"].enum_type
            assert rel is not None
            rel_totals[rel.values_by_number[tu.trip.schedule_relationship].name] += 1
            has_vehicle = tu.HasField("vehicle") and tu.vehicle.HasField("id")
            with_vehicle += has_vehicle
            with_trip_id += tu.trip.HasField("trip_id")
            stus += len(tu.stop_time_update)
            for s in tu.stop_time_update:
                ev = s.arrival if s.HasField("arrival") else s.departure
                uses_delay += ev.HasField("delay")
                uses_time += ev.HasField("time")
            if static is not None:
                agency = static.route_agency.get(tu.trip.route_id)
                if agency is None:
                    unmatched_route += 1
                name = static.agency_name.get(agency or "", "unknown route")
                per_agency[name] += 1
                per_agency_vehicle[name] += has_vehicle
                if tu.trip.HasField("trip_id") and tu.trip.schedule_relationship != 1:  # not ADDED
                    matchable += 1
                    unmatched_trip += tu.trip.trip_id not in static.trip_ids
    k = len(snaps)
    rows = [
        [name, n(cnt / k), f"{100 * cnt / total:.1f}%"] for name, cnt in rel_totals.most_common()
    ]
    out = [
        f"Averaged over {k} TripUpdates snapshots, {n(total / k)} trip updates each.",
        "",
        table(["schedule_relationship", "Per snapshot", "Share"], rows),
        "",
        f"- Trip updates naming a vehicle: **{100 * with_vehicle / total:.1f}%**",
        f"- Trip updates with a `trip_id`: {100 * with_trip_id / total:.1f}% "
        "(ADDED trips have none)",
        f"- Stop-time updates per trip update: {stus / total:.1f} on average",
        f"- Predictions given as `delay`: {uses_delay:,}; as absolute `time`: {uses_time:,}",
    ]
    if static is not None:
        out += [
            f"- Against static version `{static.feed_version}`: "
            f"{100 * unmatched_trip / max(matchable, 1):.2f}% of non-ADDED `trip_id`s and "
            f"{100 * unmatched_route / total:.2f}% of `route_id`s are not in the static file",
            "",
            table(
                ["Operator (static agency)", "Trip updates per snapshot", "Naming a vehicle"],
                [
                    [name, n(cnt / k), f"{100 * per_agency_vehicle[name] / cnt:.0f}%"]
                    for name, cnt in per_agency.most_common()
                ],
            ),
        ]
    return "\n".join(out)


def vehicles_section(
    snaps: list[tuple[dict[str, object], bytes, int]], static: Static | None
) -> str:
    total = with_trip = 0
    age: list[float] = []
    per_agency: Counter[str] = Counter()
    for hb, raw, _ in snaps:
        fetched_s = _i(hb["fetched_at_ms"]) / 1000
        for e in parse_feed(raw).entity:
            vp = e.vehicle
            total += 1
            with_trip += vp.HasField("trip") and vp.trip.HasField("trip_id")
            if vp.HasField("timestamp"):
                age.append(fetched_s - vp.timestamp)
            if static is not None:
                agency = static.route_agency.get(vp.trip.route_id)
                per_agency[static.agency_name.get(agency or "", "unknown route")] += 1
    k = len(snaps)
    out = [
        f"Averaged over {k} Vehicles snapshots, {n(total / k)} vehicles each.",
        "",
        f"- Positions linked to a trip: {100 * with_trip / total:.1f}%",
        f"- Position age at fetch (fetch time minus vehicle timestamp), s: "
        f"p50 {n(pct(age, 0.5))}, p95 {n(pct(age, 0.95))}, max {n(max(age) if age else 0)}",
    ]
    if per_agency:
        out += [
            "",
            table(
                ["Operator (static agency)", "Vehicles per snapshot"],
                [[name, n(cnt / k)] for name, cnt in per_agency.most_common()],
            ),
        ]
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", help="ISO time, e.g. 2026-11-15T19:30Z")
    parser.add_argument("--sample", type=int, default=60, help="snapshots per feed to decode")
    args = parser.parse_args(argv)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")  # operator names are Irish (Bus Éireann)
    archive = archive_from_env(os.environ)
    since_ms = (
        int(datetime.fromisoformat(args.since.replace("Z", "+00:00")).timestamp() * 1000)
        if args.since
        else 0
    )
    stats = load_heartbeats(archive, since_ms)
    if not stats:
        print("no heartbeats in the archive yet")
        return 1
    static = load_static(archive)
    per_feed = {f: snapshots(archive, stats[f], args.sample) for f in Feed if f in stats}

    print("## Collection uptime\n")
    print(uptime_section(stats, archive))
    print("\n## Snapshot size, compression and freshness\n")
    print(size_section(per_feed))
    if per_feed.get(Feed.TRIP_UPDATES):
        print("\n## TripUpdates content\n")
        print(trip_updates_section(per_feed[Feed.TRIP_UPDATES], static))
    if per_feed.get(Feed.VEHICLES):
        print("\n## Vehicles content\n")
        print(vehicles_section(per_feed[Feed.VEHICLES], static))
    return 0


if __name__ == "__main__":
    sys.exit(main())
