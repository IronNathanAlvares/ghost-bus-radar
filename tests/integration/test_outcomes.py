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
