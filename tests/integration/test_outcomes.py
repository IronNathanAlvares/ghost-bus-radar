"""Trip outcomes end to end in Spark, on a synthetic week with every outcome in it.

Timetable: the two NTA-shaped versions from ``tests/gtfs_builder.py`` (version 2 regenerates
every id). Realtime: built with the same normaliser the poller uses and passed through the same
silver frame functions as the stream and the archive backfill.
"""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from google.transit import gtfs_realtime_pb2 as pb
from pyspark.sql import SparkSession

from ghostbus_core.archive_layout import fetch_id
from ghostbus_core.feeds import Feed
from ghostbus_core.gtfsr import FetchContext, normalize
from ghostbus_core.heartbeat import Heartbeat
from ghostbus_core.lake import LakeSettings
from ghostbus_core.pacing import FetchStatus
from ghostbus_ingest.archive import LocalArchive
from ghostbus_jobs.outcomes import Sources, attach_instance, merge_outcomes, outcomes
from ghostbus_jobs.spark import build_spark
from ghostbus_jobs.stream import (
    StreamConfig,
    heartbeats_frame,
    trip_updates_frame,
    vehicle_positions_frame,
)
from ghostbus_jobs.timetable_load import Dim, run
from tests.gtfs_builder import V1_PUBLISHED, V2_PUBLISHED, archive_version

pytestmark = pytest.mark.integration

STOPS = {
    "S1": (53.3498, -6.2603),
    "S2": (53.3394, -6.2490),
    "S3": (53.3220, -6.2372),
    "S4": (53.3464, -6.2930),
}
# trip ids in tests/gtfs_builder.py: v1_0 / v2_0 = 46A 08:00, v1_6 = 39 09:00, v2_6 = 7 07:00
CFG = StreamConfig()


def at(day: date, hhmm: str) -> datetime:
    h, m = map(int, hhmm.split(":"))
    return datetime(day.year, day.month, day.day, h, m, tzinfo=UTC)  # November: Dublin = UTC


def tu_entity(
    trip_id: str, day: date, *, cancelled: bool = False, vehicle: str | None = None
) -> pb.FeedEntity:
    e = pb.FeedEntity(id="T1")
    t = e.trip_update.trip
    t.trip_id = trip_id
    t.start_date = f"{day:%Y%m%d}"
    t.schedule_relationship = (
        pb.TripDescriptor.CANCELED if cancelled else pb.TripDescriptor.SCHEDULED
    )
    if vehicle:
        e.trip_update.vehicle.id = vehicle
    return e


def vp_entity(trip_id: str, day: date, stop: str, ts: datetime) -> pb.FeedEntity:
    e = pb.FeedEntity(id="V1")
    v = e.vehicle
    v.trip.trip_id = trip_id
    v.trip.start_date = f"{day:%Y%m%d}"
    v.vehicle.id = "4242"
    v.position.latitude, v.position.longitude = STOPS[stop]
    v.timestamp = int(ts.timestamp())
    return e


class Feeds:
    """Bronze-shaped (topic, value) rows: snapshots every 2 minutes plus their heartbeats."""

    def __init__(self) -> None:
        self.rows: list[tuple[str, str]] = []

    def poll(self, feed: Feed, when: datetime, entities: list[pb.FeedEntity]) -> None:
        msg = pb.FeedMessage()
        msg.header.gtfs_realtime_version = "2.0"
        msg.header.timestamp = int(when.timestamp())
        msg.entity.extend(entities)
        ms = int(when.timestamp() * 1000)
        ctx = FetchContext(feed=feed, fetch_id=fetch_id(feed, ms), fetched_at_ms=ms)
        topic = (
            CFG.topics.trip_updates if feed is Feed.TRIP_UPDATES else CFG.topics.vehicle_positions
        )
        self.rows += [(topic, json.dumps(r.value)) for r in normalize(ctx, msg).records]
        hb = Heartbeat(
            ctx.fetch_id,
            feed,
            ms,
            FetchStatus.OK,
            200,
            100,
            10,
            int(when.timestamp()),
            len(entities),
            len(entities),
            0,
            None,
            True,
            0,
            0,
            None,
        )
        self.rows.append((CFG.topics.heartbeat, json.dumps(hb.to_json())))


def build_week(feeds: Feeds) -> None:
    d12, d13, d16, d17 = (
        date(2026, 11, 12),
        date(2026, 11, 13),
        date(2026, 11, 16),
        date(2026, 11, 17),
    )
    for day in (d12, d13, d16, d17):
        t = at(day, "06:00")
        while t <= at(day, "13:00"):
            in_gap = day == d17 and at(day, "06:50") <= t <= at(day, "08:30")
            if not in_gap:
                tu: list[pb.FeedEntity] = []
                vp: list[pb.FeedEntity] = []
                # Thu 12: 46A 08:00 runs S1 -> S2 -> S3; route 39 never appears (GHOST).
                if day == d12 and at(day, "07:50") <= t <= at(day, "08:45"):
                    tu.append(tu_entity("v1_0", day, vehicle="4242"))
                    stop = "S1" if t < at(day, "08:15") else "S2" if t < at(day, "08:35") else "S3"
                    vp.append(vp_entity("v1_0", day, stop, t - timedelta(seconds=20)))
                # Thu 12: route 39 named with a vehicle at 12:00, long after it closed. Must not
                # count: evidence stops at each trip's close time (else it is PHANTOM_LIVE).
                if day == d12 and t == at(day, "12:00"):
                    tu.append(tu_entity("v1_6", day, vehicle="555"))
                # Fri 13: 46A cancelled at 07:00 (EARLY); route 39 tracked at S3 only (VANISHED).
                if day == d13 and t >= at(day, "07:00") and t <= at(day, "08:40"):
                    tu.append(tu_entity("v1_0", day, cancelled=True))
                if day == d13 and at(day, "08:58") <= t <= at(day, "09:06"):
                    vp.append(vp_entity("v1_6", day, "S3", t - timedelta(seconds=20)))
                # Mon 16 (version 2 ids): 46A cancelled at 07:58 (LATE); route 7 assigned a
                # vehicle but never tracked (PHANTOM_LIVE).
                if day == d16 and at(day, "07:58") <= t <= at(day, "08:40"):
                    tu.append(tu_entity("v2_0", day, cancelled=True))
                if day == d16 and at(day, "06:40") <= t <= at(day, "07:45"):
                    tu.append(tu_entity("v2_6", day, vehicle="999"))
                # Tue 17: nothing seen, but our collection was down 06:50-08:30 (NO_DATA).
                feeds.poll(Feed.TRIP_UPDATES, t, tu)
                feeds.poll(Feed.VEHICLES, t + timedelta(seconds=30), vp)
            t += timedelta(minutes=2)


@pytest.fixture(scope="module")
def result(
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[tuple[SparkSession, Dim, dict[tuple[date, str], dict[str, object]]]]:
    spark = build_spark("it-outcomes", LakeSettings.from_env(dict(os.environ)))
    tmp = tmp_path_factory.mktemp("outcomes")
    archive = LocalArchive(tmp / "archive")
    archive_version(archive, 1, V1_PUBLISHED, tmp)
    archive_version(archive, 2, V2_PUBLISHED, tmp)
    dim = Dim(f"lake.it_{uuid.uuid4().hex[:8]}")
    run(spark, archive, tmp / "work", date(2026, 11, 12), date(2026, 11, 17), dim=dim)

    feeds = Feeds()
    build_week(feeds)
    raw = spark.createDataFrame(feeds.rows, "topic STRING, value STRING")
    src = Sources(
        trip_updates_frame(raw, CFG),
        vehicle_positions_frame(raw, CFG),
        heartbeats_frame(raw, CFG),
        "test",
    )
    clock = at(date(2026, 11, 17), "13:00")
    df = outcomes(
        spark,
        src,
        date(2026, 11, 12),
        date(2026, 11, 17),
        clock,
        "final",
        dim=dim,
        horizon_start=at(date(2026, 11, 12), "05:00"),
    )
    gold = f"{dim.ns}_gold.trip_outcomes"
    merge_outcomes(spark, df, gold)
    rows = {}
    for r in spark.table(gold).collect():
        short = r["route_key"].split("|")[1]
        rows[(r["service_date"], f"{short} {str(r['scheduled_start'])[11:16]}")] = r.asDict()
    yield spark, dim, rows
    for t in [*dim.all(), gold]:
        spark.sql(f"DROP TABLE IF EXISTS {t} PURGE")


@pytest.mark.parametrize(
    ("day", "trip", "expected"),
    [
        (date(2026, 11, 12), "46A 08:00", "RAN"),
        (date(2026, 11, 12), "39 09:00", "GHOST"),
        (date(2026, 11, 13), "46A 08:00", "CANCELLED_EARLY"),
        (date(2026, 11, 13), "39 09:00", "VANISHED"),
        (date(2026, 11, 16), "46A 08:00", "CANCELLED_LATE"),
        (date(2026, 11, 16), "7 07:00", "PHANTOM_LIVE"),
        (date(2026, 11, 17), "46A 08:00", "NO_DATA"),
        (date(2026, 11, 17), "7 07:00", "NO_DATA"),
        (date(2026, 11, 12), "rail 10:00", "OUT_OF_SCOPE"),
        (date(2026, 11, 12), "Green 10:25", "OUT_OF_SCOPE"),
        (date(2026, 11, 12), "46A 00:30", "NO_DATA"),  # 24:30 trip: we were not collecting at night
    ],
)
def test_every_outcome(
    result: tuple[SparkSession, Dim, dict[tuple[date, str], dict[str, object]]],
    day: date,
    trip: str,
    expected: str,
) -> None:
    _, _, rows = result
    row = rows[(day, trip)]
    assert row["outcome"] == expected, row["reason"]


def test_evidence_columns(
    result: tuple[SparkSession, Dim, dict[tuple[date, str], dict[str, object]]],
) -> None:
    _, _, rows = result
    ran = rows[(date(2026, 11, 12), "46A 08:00")]
    assert ran["progress"] == 1.0 and int(str(ran["vp_positions"])) >= 20 and ran["tu_vehicle"]
    ghost = rows[(date(2026, 11, 12), "39 09:00")]
    assert ghost["tu_snapshots"] == 0 and not ghost["tu_vehicle"]  # 12:00 sighting ignored
    vanished = rows[(date(2026, 11, 13), "39 09:00")]
    assert vanished["progress"] == 0.5
    late = rows[(date(2026, 11, 16), "46A 08:00")]
    assert "2 min" in str(late["reason"])  # cancelled at 07:58 for an 08:00 departure
    assert all(r["status"] == "final" and r["source"] == "test" for r in rows.values())


def test_provisional_never_overwrites_final(
    result: tuple[SparkSession, Dim, dict[tuple[date, str], dict[str, object]]], tmp_path: Path
) -> None:
    spark, dim, rows = result
    gold = f"{dim.ns}_gold.trip_outcomes"
    before = spark.table(gold).count()
    key = rows[(date(2026, 11, 12), "39 09:00")]["trip_instance_key"]
    fake = (
        spark.table(gold)
        .where(f"trip_instance_key = '{key}'")
        .selectExpr(
            *[
                "'RAN' AS outcome"
                if c == "outcome"
                else "'provisional' AS status"
                if c == "status"
                else c
                for c in spark.table(gold).columns
            ]
        )
    )
    merge_outcomes(spark, fake, gold)
    assert spark.table(gold).where(f"trip_instance_key = '{key}'").first()["outcome"] == "GHOST"  # type: ignore[index]
    assert spark.table(gold).count() == before


def test_old_trip_ids_just_after_a_publish_still_find_their_trip(
    result: tuple[SparkSession, Dim, dict[tuple[date, str], dict[str, object]]],
) -> None:
    """The realtime feed can keep the previous version's ids after a new timetable is published
    (76 minutes on 15-16 Nov 2026). An old id 40 minutes after the publish must still attach to
    its trip; 4 hours after, the grace window has ended and it must not."""
    spark, dim, rows = result
    expected = rows[(date(2026, 11, 16), "46A 08:00")]["trip_instance_key"]
    rt = spark.createDataFrame(
        [
            ((V2_PUBLISHED + timedelta(minutes=40)).replace(tzinfo=None), "v1_0", "20261116"),
            ((V2_PUBLISHED + timedelta(hours=4)).replace(tzinfo=None), "v1_0", "20261116"),
            ((V2_PUBLISHED + timedelta(minutes=40)).replace(tzinfo=None), "v2_0", "20261116"),
        ],
        "fetched_at TIMESTAMP, trip_id STRING, start_date STRING",
    )
    got = {
        (r["fetched_at"], r["trip_id"]): (r["trip_instance_key"], r["version_fallback"])
        for r in attach_instance(rt, spark, dim).collect()
    }
    t40 = (V2_PUBLISHED + timedelta(minutes=40)).replace(tzinfo=None)
    assert got[(t40, "v1_0")] == (expected, True)  # old id, inside the grace window
    assert got[(t40, "v2_0")] == (expected, False)  # new id, as before
    assert (V2_PUBLISHED.replace(tzinfo=None) + timedelta(hours=4), "v1_0") not in got


def test_quality_gather_counts_the_synthetic_day_exactly(
    result: tuple[SparkSession, Dim, dict[tuple[date, str], dict[str, object]]],
) -> None:
    """The nightly quality job on a known day: every number it reports, checked by hand."""
    from ghostbus_core.quality import evaluate
    from ghostbus_jobs.quality import gather, write

    spark, dim, _ = result
    feeds = Feeds()
    build_week(feeds)
    raw = spark.createDataFrame(feeds.rows, "topic STRING, value STRING")
    src = Sources(
        trip_updates_frame(raw, CFG),
        vehicle_positions_frame(raw, CFG),
        heartbeats_frame(raw, CFG),
        "test",
    )
    day = date(2026, 11, 12)
    gold = f"{dim.ns}_gold.trip_outcomes"
    stats = gather(spark, day, src, gold=gold, dim=dim)

    assert (
        stats.scheduled == spark.table(dim.trip_instance).where(f"service_date = '{day}'").count()
    )
    # (the fixture's ``rows`` dict is keyed by route and time, so same-label twins collapse)
    assert stats.decided == spark.table(gold).where(f"service_date = '{day}'").count()
    # Polled 06:00 to 13:00 every 2 minutes, both feeds inside the same minute: 211 minutes.
    assert (stats.minutes_collected, stats.minutes_expected) == (211, 1440)
    assert stats.rt_with_id > 0 and stats.rt_unmatched == 0
    assert stats.scheduled_week_away is None  # only 12 to 17 Nov are loaded

    checks = evaluate(stats)
    table = f"{dim.ns}_gold.data_quality"
    write(spark, day, checks, table=table)
    write(spark, day, checks, table=table)  # a rerun updates, never duplicates
    stored = spark.table(table).where(f"service_date = '{day}'").collect()
    assert len(stored) == len(checks)
    assert {r["check_name"]: r["status"] for r in stored}["completeness"] == "fail"
    spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")


def test_export_end_to_end_on_the_synthetic_week(
    result: tuple[SparkSession, Dim, dict[tuple[date, str], dict[str, object]]], tmp_path: Path
) -> None:
    """The nightly export against real Spark tables: a passed day is published, a failed one is
    listed but excluded, every file matches its schema, and a rerun uploads nothing new."""
    import jsonschema

    from ghostbus_core import schemas
    from ghostbus_core.quality import Check
    from ghostbus_jobs.export import publish, run
    from ghostbus_jobs.quality import write

    spark, dim, _ = result
    gold = f"{dim.ns}_gold.trip_outcomes"
    qt = f"{dim.ns}_gold.data_quality"
    write(spark, date(2026, 11, 12), [Check("completeness", 1.0, "ok", "all minutes")], table=qt)
    write(
        spark, date(2026, 11, 13), [Check("completeness", 0.2, "fail", "laptop asleep")], table=qt
    )
    site = LocalArchive(tmp_path / "site")
    try:
        docs = run(spark, date(2026, 11, 17), site, None, gold=gold, quality_table=qt, dim=dim)
        assert docs["days/2026-11-12.json"]["published"] is True
        assert docs["days/2026-11-13.json"]["published"] is False
        assert docs["meta.json"]["latest_published_day"] == "2026-11-12"
        codes = {s["code"] for s in docs["stops.json"]["stops"]}
        assert {"101", "102", "103"} <= codes
        for path, doc in docs.items():
            name = next(v for k, v in schemas.EXPORT.items() if path.startswith(k))
            jsonschema.validate(doc, schemas.load(name))
        assert (tmp_path / "site" / "league" / "7d.json").exists()
        written, unchanged = publish(site, docs)  # same documents again
        assert (written, unchanged) == (0, len(docs))
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {qt} PURGE")


def test_api_index_and_the_api_on_real_spark_tables(
    result: tuple[SparkSession, Dim, dict[tuple[date, str], dict[str, object]]], tmp_path: Path
) -> None:
    """The nightly stop index from real dim tables, read back by the API Lambda: buses only, no
    departure from a trip's last stop, GTFS times past 24:00 on the right morning, and the trip
    ids of the version that defines each day (v2 from 15 November)."""
    import jsonschema

    from ghostbus_api.handler import Sources, departures
    from ghostbus_core import schemas
    from ghostbus_core.departures import POINTER_KEY, read_shard, shard_key, shard_of
    from ghostbus_jobs.api_index import run as build_index

    spark, dim, _ = result
    target = LocalArchive(tmp_path / "api")
    docs = build_index(spark, date(2026, 11, 16), target, build="20261116T042000Z", dim=dim)
    pointer = json.loads(docs[POINTER_KEY])
    jsonschema.validate(pointer, schemas.load(schemas.STOP_INDEX_POINTER))
    # Mon 16 and Tue 17: 46A 08:00 (S1, S2), 46A 24:30 (S1), route 7 07:00 (S4). S3 is only ever
    # a last stop; rail and Luas are out of scope.
    assert (pointer["stops"], pointer["departures"]) == (3, 8)
    assert pointer["valid_until"] == "2026-11-18T04:00:00Z"  # Wed 18 Nov, the first day not covered
    _, deps = read_shard(docs[shard_key("20261116T042000Z", shard_of("101"))])
    s1 = sorted((d.at, d.trip.route, d.trip.trip_id, d.trip.start_time) for d in deps["101"])
    first = int(at(date(2026, 11, 16), "08:00").timestamp())
    late = int(at(date(2026, 11, 17), "00:30").timestamp())
    assert s1[:2] == [(first, "46A", "v2_0", "08:00:00"), (late, "46A", "v2_1", "24:30:00")]
    assert (tmp_path / "api" / POINTER_KEY).exists()

    empty = LocalArchive(tmp_path / "nothing")
    src = Sources(target, empty, empty, clock=lambda: first - 300)
    body = departures({"pathParameters": {"code": "101"}}, src)
    jsonschema.validate(body, schemas.load(schemas.DEPARTURES))
    assert [(r["route"], r["status"]) for r in body["departures"]] == [("46A", "no_live_data")]
