"""Bronze and silver on throwaway topics and namespaces, with the available-now trigger."""

from __future__ import annotations

import os
import time
import uuid
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ghostbus_core.feeds import Feed
from ghostbus_core.gtfsr import FetchContext, normalize
from ghostbus_core.lake import LakeSettings
from ghostbus_ingest.kafka_config import bootstrap_from_env
from ghostbus_jobs import stream_schemas as S
from ghostbus_jobs.spark import build_spark
from ghostbus_jobs.stream import StreamConfig, run_available_now
from ghostbus_jobs.stream_metrics import MetricsListener
from tests import builders as b
from tests.stream_data import make_topics, produce, snapshot, trip, vehicle

pytestmark = pytest.mark.integration

BOOTSTRAP = bootstrap_from_env(os.environ)
T0 = 1_794_770_400  # 2026-11-15 19:20:00 UTC


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    yield build_spark("it-stream", LakeSettings.from_env(dict(os.environ)))


@pytest.fixture
def cfg(spark: SparkSession, tmp_path: Path) -> Iterator[StreamConfig]:
    tag = uuid.uuid4().hex[:8]
    c = StreamConfig(
        bronze_ns=f"lake.it_{tag}_bronze",
        silver_ns=f"lake.it_{tag}_silver",
        topics=make_topics(BOOTSTRAP),
        checkpoint_root=tmp_path / "checkpoints",
        fail_on_data_loss=True,
    )
    yield c
    for ns in (c.bronze_ns, c.silver_ns):
        for t in spark.sql(f"SHOW TABLES IN {ns}").collect():
            spark.sql(f"DROP TABLE IF EXISTS {ns}.{t['tableName']} PURGE")
        spark.sql(f"DROP NAMESPACE IF EXISTS {ns}")


def rows(spark: SparkSession, table: str) -> list[dict[str, Any]]:
    spark.sql(f"REFRESH TABLE {table}")
    return [r.asDict(recursive=True) for r in spark.table(table).collect()]


@pytest.mark.parametrize(
    ("feed", "schema", "entities"),
    [
        (
            Feed.TRIP_UPDATES,
            S.TRIP_UPDATE,
            [b.scheduled_trip(), b.cancelled_trip(), b.added_trip()],
        ),
        (Feed.VEHICLES, S.VEHICLE_POSITION, [b.vehicle(), b.vehicle("V2", with_trip=False)]),
    ],
)
def test_spark_schemas_lose_nothing(
    spark: SparkSession, feed: Feed, schema: str, entities: list[object]
) -> None:
    """Every field the normaliser emits survives from_json with the Spark schema."""
    import json

    ctx = FetchContext(feed=feed, fetch_id=f"{feed.value}:1", fetched_at_ms=1)
    values = [r.value for r in normalize(ctx, b.feed(*entities)).records]  # type: ignore[arg-type]
    df = spark.createDataFrame([(json.dumps(v),) for v in values], "value STRING")
    parsed = df.select(F.to_json(F.from_json("value", schema)).alias("j")).collect()
    for original, row in zip(values, parsed, strict=True):
        back = json.loads(row["j"])

        def strip_nulls(x: object) -> object:
            if isinstance(x, dict):
                return {k: strip_nulls(v) for k, v in x.items() if v is not None}
            if isinstance(x, list):
                return [strip_nulls(v) for v in x]
            return x

        assert strip_nulls(back) == strip_nulls(original)


def test_pipeline_dedupes_and_explodes(spark: SparkSession, cfg: StreamConfig) -> None:
    t = cfg.topics
    tu1 = snapshot(Feed.TRIP_UPDATES, t, T0, [trip("T1", "A", 3), trip("T2", "B", 2)])
    tu2 = snapshot(Feed.TRIP_UPDATES, t, T0 + 120, [trip("T1", "A", 2), trip("T2", "B", 1)])
    # Vehicle 7139 reports at T0-30 and is repeated unchanged (stale) in the next snapshot.
    vp1 = snapshot(Feed.VEHICLES, t, T0 + 60, [vehicle("V1", "7139", "A", T0 - 30)])
    vp2 = snapshot(
        Feed.VEHICLES,
        t,
        T0 + 180,
        [vehicle("V1", "7139", "A", T0 - 30), vehicle("V2", "8", "B", T0 + 170)],
    )
    replay = tu1  # the same fetch published twice (a poller retry, a replay from the archive)
    produce(BOOTSTRAP, tu1 + tu2 + vp1 + vp2 + replay)
    run_available_now(spark, cfg, BOOTSTRAP)

    bronze = rows(spark, cfg.bronze)
    assert len(bronze) == len(tu1 + tu2 + vp1 + vp2 + replay)
    assert len({(r["topic"], r["kafka_partition"], r["kafka_offset"]) for r in bronze}) == len(
        bronze
    )

    tus = rows(spark, cfg.silver("trip_updates"))
    assert sorted((r["trip_key"], r["event_time"].timestamp()) for r in tus) == sorted(
        [("20261115|A", T0), ("20261115|B", T0), ("20261115|A", T0 + 120), ("20261115|B", T0 + 120)]
    )  # the replayed fetch is gone
    vps = rows(spark, cfg.silver("vehicle_positions"))
    assert sorted(r["vehicle_id"] for r in vps) == ["7139", "8"]  # the stale repeat is gone
    stale = next(r for r in vps if r["vehicle_id"] == "7139")
    assert stale["position_age_s"] == 90 and stale["fetch_id"] == f"vehicles:{(T0 + 60) * 1000}"

    stps = rows(spark, cfg.silver("stop_time_predictions"))
    assert len(stps) == 3 + 2 + 2 + 1
    first = next(r for r in stps if r["trip_id"] == "A" and r["stop_sequence"] == 3)
    assert first["arrival_delay"] == 60 and first["stop_id"] == "STOP2"

    hbs = rows(spark, cfg.silver("heartbeats"))
    assert len(hbs) == 4 and all(r["collected"] for r in hbs)

    # Running again with nothing new changes nothing.
    run_available_now(spark, cfg, BOOTSTRAP)
    assert len(rows(spark, cfg.bronze)) == len(bronze)
    assert len(rows(spark, cfg.silver("stop_time_predictions"))) == len(stps)


def test_a_correction_upstream_does_not_stop_the_stream(
    spark: SparkSession, cfg: StreamConfig
) -> None:
    """Deleting bad rows from every layer (a correction) must not stop the stream. Iceberg's
    streaming source fails on delete and overwrite snapshots unless told to skip them; found in
    Sprint 4 when test data that had leaked into the live tables had to be removed."""
    t = cfg.topics
    bad = snapshot(Feed.TRIP_UPDATES, t, T0, [trip("T1", "A", 3)])
    keep = snapshot(Feed.TRIP_UPDATES, t, T0 + 120, [trip("T2", "B", 2)])
    produce(BOOTSTRAP, bad + keep)
    run_available_now(spark, cfg, BOOTSTRAP)
    fid = f"trip_updates:{T0 * 1000}"
    layers = [cfg.bronze, *(cfg.silver(n) for n in ("trip_updates", "heartbeats"))]
    for table in [*layers, cfg.silver("stop_time_predictions")]:
        spark.sql(f"DELETE FROM {table} WHERE fetch_id = '{fid}'")

    later = snapshot(Feed.TRIP_UPDATES, t, T0 + 240, [trip("T3", "C", 1)])
    produce(BOOTSTRAP, later)
    run_available_now(spark, cfg, BOOTSTRAP)  # raised before the fix
    tus = rows(spark, cfg.silver("trip_updates"))
    assert sorted(r["trip_key"] for r in tus) == ["20261115|B", "20261115|C"]
    assert all(r["fetch_id"] != fid for table in layers for r in rows(spark, table))


def test_late_events(spark: SparkSession, cfg: StreamConfig) -> None:
    """5 min late: used. 30 min late: dropped from silver, counted, and still in bronze."""
    listener = MetricsListener()
    spark.streams.addListener(listener)
    try:
        t = cfg.topics
        minute = 60
        produce(
            BOOTSTRAP,
            snapshot(Feed.VEHICLES, t, T0, [vehicle("V1", "A1", "a", T0)])
            + snapshot(
                Feed.VEHICLES, t, T0 + 20 * minute, [vehicle("V1", "B1", "b", T0 + 20 * minute)]
            ),
        )
        run_available_now(spark, cfg, BOOTSTRAP)
        # Max event time is now T0+20m, so the watermark is T0+10m.
        produce(
            BOOTSTRAP,
            snapshot(
                Feed.VEHICLES,
                t,
                T0 + 21 * minute,
                [
                    vehicle("V1", "LATE5", "c", T0 + 15 * minute),  # 5 min behind the max: kept
                    vehicle("V2", "LATE30", "d", T0 - 10 * minute),  # 30 min behind: too late
                ],
            ),
        )
        run_available_now(spark, cfg, BOOTSTRAP)

        silver = {r["vehicle_id"] for r in rows(spark, cfg.silver("vehicle_positions"))}
        assert silver == {"A1", "B1", "LATE5"}
        raw = " ".join(str(r["value"]) for r in rows(spark, cfg.bronze))
        assert "LATE30" in raw  # not lost: bronze (and the archive) keep it for reconcile

        deadline = time.time() + 30  # listener events arrive asynchronously
        while (
            time.time() < deadline and listener.total("silver_vehicle_positions", "late_rows") < 1
        ):
            time.sleep(0.5)
        assert listener.total("silver_vehicle_positions", "late_rows") == 1
    finally:
        spark.streams.removeListener(listener)


def test_config_is_frozen() -> None:
    c = StreamConfig()
    assert replace(c, trigger="available-now").trigger == "available-now"
    assert c.bronze == "lake.bronze.gtfsr_raw"
