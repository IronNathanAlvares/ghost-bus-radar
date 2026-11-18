"""The streaming pipeline: Kafka -> bronze -> silver, exactly once, with event-time watermarks.

Queries (each with its own checkpoint, all writing through Iceberg's streaming sink):

``bronze``
    Kafka (all feed topics) to ``bronze.gtfsr_raw``: the JSON payload untouched plus Kafka
    lineage. Kafka offsets live in the checkpoint and Iceberg records the query id and epoch in
    every snapshot, skipping an epoch it already committed, so a restart cannot write twice.
``silver_trip_updates``, ``silver_vehicle_positions``, ``silver_heartbeats``
    Bronze (read as an Iceberg stream) to typed silver tables. Each gets an event-time column,
    a watermark (default 10 minutes) and ``dropDuplicatesWithinWatermark`` on a natural key:
    replayed fetches disappear, and so do the stale vehicle positions NTA repeats for minutes.
    Rows older than the watermark are dropped here *and counted*; they remain in bronze and the
    archive, where the nightly reconcile (Sprint 4) picks them up.
``silver_stop_time_predictions``
    Silver trip updates exploded to one row per predicted stop. Stateless.

Usage::

    python -m ghostbus_jobs.stream                         # continuous, 30 s trigger
    python -m ghostbus_jobs.stream --available-now         # process what is there, then stop
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.query import StreamingQuery

from ghostbus_core.lake import LakeSettings, table_properties
from ghostbus_core.topics import ALERTS, HEARTBEAT, TRIP_UPDATES, VEHICLE_POSITIONS
from ghostbus_ingest.kafka_config import bootstrap_from_env
from ghostbus_jobs import stream_schemas as S
from ghostbus_jobs.spark import build_spark
from ghostbus_jobs.stream_metrics import MetricsListener

log = logging.getLogger("ghostbus.stream")


@dataclass(frozen=True)
class Topics:
    trip_updates: str = TRIP_UPDATES.name
    vehicle_positions: str = VEHICLE_POSITIONS.name
    alerts: str = ALERTS.name
    heartbeat: str = HEARTBEAT.name

    def all(self) -> list[str]:
        return [self.trip_updates, self.vehicle_positions, self.alerts, self.heartbeat]


@dataclass(frozen=True)
class StreamConfig:
    bronze_ns: str = "lake.bronze"
    silver_ns: str = "lake.silver"
    topics: Topics = field(default_factory=Topics)
    checkpoint_root: Path = Path("/checkpoints/stream")
    trigger: str = "30 seconds"  # or "available-now"
    watermark: str = "10 minutes"
    starting_offsets: str = "earliest"
    #: Kafka is transport; a gap (topic recreated, retention passed) is closed from the archive.
    fail_on_data_loss: bool = False
    max_offsets_per_trigger: int | None = None
    max_files_per_batch: int | None = None

    @property
    def bronze(self) -> str:
        return f"{self.bronze_ns}.gtfsr_raw"

    def silver(self, name: str) -> str:
        return f"{self.silver_ns}.{name}"

    def checkpoint(self, query: str) -> str:
        return str(self.checkpoint_root / query)


SILVER_TABLES = ("trip_updates", "vehicle_positions", "heartbeats", "stop_time_predictions")
QUERY_ORDER = (
    "bronze",
    "silver_trip_updates",
    "silver_vehicle_positions",
    "silver_heartbeats",
    "silver_stop_time_predictions",
)


# --- tables -------------------------------------------------------------------------------------


def _props() -> str:
    return ", ".join(f"'{k}'='{v}'" for k, v in table_properties().items())


def ensure_table(spark: SparkSession, table: str, df: DataFrame, partition: str) -> None:
    """Create ``table`` from a (streaming) DataFrame's schema if it does not exist."""
    cols = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in df.schema.fields)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {table.rsplit('.', 1)[0]}")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} ({cols}) USING iceberg "
        f"PARTITIONED BY ({partition}) TBLPROPERTIES ({_props()})"
    )


def _write(
    spark: SparkSession, df: DataFrame, cfg: StreamConfig, query: str, table: str, partition: str
) -> StreamingQuery:
    ensure_table(spark, table, df, partition)
    writer = (
        df.writeStream.format("iceberg")
        .queryName(query)
        .outputMode("append")
        .option("checkpointLocation", cfg.checkpoint(query))
        .option("fanout-enabled", "true")
    )
    if cfg.trigger == "available-now":
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=cfg.trigger)
    return writer.toTable(table)


#: Iceberg's streaming source fails on a snapshot that deletes or rewrites rows. Corrections
#: (removing bad data) are applied to every layer explicitly, so readers skip those snapshots
#: instead of stopping. Compaction ("replace" snapshots) is skipped by Iceberg already.
SKIP_CORRECTIONS = {
    "streaming-skip-delete-snapshots": "true",
    "streaming-skip-overwrite-snapshots": "true",
}


def iceberg_stream(spark: SparkSession, table: str, max_files: int | None = None) -> DataFrame:
    reader = spark.readStream.format("iceberg").options(**SKIP_CORRECTIONS)
    if max_files:
        reader = reader.option("streaming-max-files-per-micro-batch", max_files)
    return reader.load(table)


def _iceberg_stream(spark: SparkSession, cfg: StreamConfig, table: str) -> DataFrame:
    return iceberg_stream(spark, table, cfg.max_files_per_batch)


# --- bronze -------------------------------------------------------------------------------------


def bronze_frame(spark: SparkSession, cfg: StreamConfig, bootstrap: str) -> DataFrame:
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", ",".join(cfg.topics.all()))
        .option("startingOffsets", cfg.starting_offsets)
        .option("failOnDataLoss", str(cfg.fail_on_data_loss).lower())
    )
    if cfg.max_offsets_per_trigger:
        reader = reader.option("maxOffsetsPerTrigger", cfg.max_offsets_per_trigger)
    value = F.col("value").cast("string")
    return reader.load().select(
        "topic",
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").cast("string").alias("key"),
        value.alias("value"),
        F.get_json_object(value, "$.fetch_id").alias("fetch_id"),
        F.timestamp_millis(F.get_json_object(value, "$.fetched_at_ms").cast("bigint")).alias(
            "fetched_at"
        ),
        F.current_timestamp().alias("ingested_at"),
    )


def start_bronze(spark: SparkSession, cfg: StreamConfig, bootstrap: str) -> StreamingQuery:
    df = bronze_frame(spark, cfg, bootstrap)
    return _write(spark, df, cfg, "bronze", cfg.bronze, "topic, days(fetched_at)")


# --- silver -------------------------------------------------------------------------------------


def _seconds(col: Column | str) -> Column:
    return F.timestamp_seconds(col)


def _trip_cols(prefix: str = "v.trip") -> list[Column]:
    return [
        F.col(f"{prefix}.trip_id").alias("trip_id"),
        F.col(f"{prefix}.route_id").alias("route_id"),
        F.col(f"{prefix}.direction_id").alias("direction_id"),
        F.col(f"{prefix}.start_time").alias("start_time"),
        F.col(f"{prefix}.start_date").alias("start_date"),
        F.col(f"{prefix}.schedule_relationship").alias("schedule_relationship"),
    ]


def _parsed(bronze: DataFrame, topic: str, schema: str) -> DataFrame:
    """Rows of one topic with the payload parsed. Unparseable payloads stay in bronze only."""
    return (
        bronze.where(F.col("topic") == topic)
        .select(F.from_json("value", schema).alias("v"))
        .where(F.col("v.fetch_id").isNotNull())
    )


def _dedupe(df: DataFrame, time_col: str, keys: list[str], cfg: StreamConfig) -> DataFrame:
    """Drop duplicate observations of ``keys``.

    Streaming: watermark-bounded state (``dropDuplicatesWithinWatermark``). Batch (reconcile and
    backfill over the archive): every row is present, so a plain ``dropDuplicates`` on the same
    key gives the same result; Spark 3.5 rejects the watermark variant on batch frames.
    """
    if df.isStreaming:
        return df.withWatermark(time_col, cfg.watermark).dropDuplicatesWithinWatermark(keys)
    return df.dropDuplicates(keys)


def trip_updates_frame(bronze: DataFrame, cfg: StreamConfig) -> DataFrame:
    """Event time = the feed header time: NTA stamps every trip update with it (measurement 01)."""
    v = _parsed(bronze, cfg.topics.trip_updates, S.TRIP_UPDATE)
    return _dedupe(
        v.select(
            _seconds(F.coalesce("v.feed_timestamp", F.col("v.fetched_at_ms") / 1000)).alias(
                "event_time"
            ),
            F.col("v.fetch_id").alias("fetch_id"),
            F.timestamp_millis("v.fetched_at_ms").alias("fetched_at"),
            _seconds("v.feed_timestamp").alias("feed_timestamp"),
            F.col("v.trip_key").alias("trip_key"),
            F.col("v.entity_id").alias("entity_id"),
            *_trip_cols(),
            F.col("v.vehicle.id").alias("vehicle_id"),
            F.col("v.delay").alias("delay"),
            F.col("v.stop_time_updates").alias("stop_time_updates"),
        ),
        "event_time",
        ["trip_key", "event_time"],
        cfg,
    )


def vehicle_positions_frame(bronze: DataFrame, cfg: StreamConfig) -> DataFrame:
    """Event time = the vehicle's own timestamp. A position repeated across snapshots (same
    vehicle, trip and timestamp) is one observation, not several."""
    v = _parsed(bronze, cfg.topics.vehicle_positions, S.VEHICLE_POSITION)
    event = _seconds(F.coalesce("v.timestamp", "v.feed_timestamp"))
    return _dedupe(
        v.select(
            event.alias("event_time"),
            F.col("v.fetch_id").alias("fetch_id"),
            F.timestamp_millis("v.fetched_at_ms").alias("fetched_at"),
            _seconds("v.feed_timestamp").alias("feed_timestamp"),
            F.col("v.trip_key").alias("trip_key"),
            F.col("v.entity_id").alias("entity_id"),
            *_trip_cols(),
            F.coalesce(F.col("v.vehicle.id"), F.lit("")).alias("vehicle_id"),
            F.col("v.position.latitude").alias("latitude"),
            F.col("v.position.longitude").alias("longitude"),
            F.col("v.position.bearing").alias("bearing"),
            F.col("v.position.speed").alias("speed"),
            F.col("v.current_stop_sequence").alias("current_stop_sequence"),
            F.col("v.stop_id").alias("stop_id"),
            F.col("v.current_status").alias("current_status"),
            (F.col("v.fetched_at_ms") / 1000 - F.coalesce("v.timestamp", "v.feed_timestamp"))
            .cast("int")
            .alias("position_age_s"),
        ),
        "event_time",
        ["vehicle_id", "trip_key", "event_time"],
        cfg,
    )


def heartbeats_frame(bronze: DataFrame, cfg: StreamConfig) -> DataFrame:
    v = _parsed(bronze, cfg.topics.heartbeat, S.HEARTBEAT)
    return _dedupe(
        v.select(
            F.timestamp_millis("v.fetched_at_ms").alias("fetched_at"),
            *[
                F.col(f"v.{c}").alias(c)
                for c in (
                    "fetch_id",
                    "feed",
                    "status",
                    "http_status",
                    "elapsed_ms",
                    "bytes",
                    "entity_count",
                    "records",
                    "entity_errors",
                    "archived",
                    "collected",
                    "kafka_delivered",
                    "kafka_unconfirmed",
                    "error",
                )
            ],
            _seconds("v.feed_timestamp").alias("feed_timestamp"),
        ),
        "fetched_at",
        ["fetch_id"],
        cfg,
    )


def stop_time_predictions_frame(trip_updates: DataFrame) -> DataFrame:
    stu = F.explode("stop_time_updates").alias("s")
    return trip_updates.select(
        "event_time", "fetch_id", "trip_key", "trip_id", "start_date", "schedule_relationship", stu
    ).select(
        "event_time",
        "fetch_id",
        "trip_key",
        "trip_id",
        "start_date",
        F.col("schedule_relationship").alias("trip_schedule_relationship"),
        F.col("s.stop_sequence").alias("stop_sequence"),
        F.col("s.stop_id").alias("stop_id"),
        F.col("s.arrival.delay").alias("arrival_delay"),
        _seconds("s.arrival.time").alias("arrival_time"),
        F.col("s.departure.delay").alias("departure_delay"),
        _seconds("s.departure.time").alias("departure_time"),
        F.col("s.schedule_relationship").alias("stop_schedule_relationship"),
    )


def start_silver(spark: SparkSession, cfg: StreamConfig, name: str) -> StreamingQuery:
    if name == "silver_stop_time_predictions":
        tu = _iceberg_stream(spark, cfg, cfg.silver("trip_updates"))
        df = stop_time_predictions_frame(tu)
        return _write(spark, df, cfg, name, cfg.silver("stop_time_predictions"), "days(event_time)")
    bronze = _iceberg_stream(spark, cfg, cfg.bronze)
    frames = {
        "silver_trip_updates": (trip_updates_frame, "trip_updates", "days(event_time)"),
        "silver_vehicle_positions": (
            vehicle_positions_frame,
            "vehicle_positions",
            "days(event_time)",
        ),
        "silver_heartbeats": (heartbeats_frame, "heartbeats", "days(fetched_at)"),
    }
    build, table, partition = frames[name]
    return _write(spark, build(bronze, cfg), cfg, name, cfg.silver(table), partition)


def ensure_all_tables(spark: SparkSession, cfg: StreamConfig, bootstrap: str) -> None:
    """Create every table up front, so a silver query never starts before its source exists."""
    bronze_df = bronze_frame(spark, cfg, bootstrap)
    ensure_table(spark, cfg.bronze, bronze_df, "topic, days(fetched_at)")
    src = spark.table(cfg.bronze)
    ensure_table(
        spark, cfg.silver("trip_updates"), trip_updates_frame(src, cfg), "days(event_time)"
    )
    ensure_table(
        spark,
        cfg.silver("vehicle_positions"),
        vehicle_positions_frame(src, cfg),
        "days(event_time)",
    )
    ensure_table(spark, cfg.silver("heartbeats"), heartbeats_frame(src, cfg), "days(fetched_at)")
    tu = spark.table(cfg.silver("trip_updates"))
    ensure_table(
        spark,
        cfg.silver("stop_time_predictions"),
        stop_time_predictions_frame(tu),
        "days(event_time)",
    )


# --- running ------------------------------------------------------------------------------------


def run_available_now(
    spark: SparkSession, cfg: StreamConfig, bootstrap: str, queries: Sequence[str] = QUERY_ORDER
) -> None:
    """Process everything currently available, query by query in dependency order, then stop."""
    cfg = replace(cfg, trigger="available-now")
    ensure_all_tables(spark, cfg, bootstrap)
    for name in queries:
        q = (
            start_bronze(spark, cfg, bootstrap)
            if name == "bronze"
            else start_silver(spark, cfg, name)
        )
        q.awaitTermination()
        if q.exception():
            raise RuntimeError(f"{name} failed: {q.exception()}")


def run_continuous(spark: SparkSession, cfg: StreamConfig, bootstrap: str) -> None:
    ensure_all_tables(spark, cfg, bootstrap)
    queries = [start_bronze(spark, cfg, bootstrap)]
    queries += [start_silver(spark, cfg, n) for n in QUERY_ORDER[1:]]
    log.info("running %d queries, trigger %s", len(queries), cfg.trigger)
    spark.streams.awaitAnyTermination()
    for q in queries:
        if q.exception():
            raise RuntimeError(f"{q.name} failed: {q.exception()}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trigger", default="30 seconds")
    parser.add_argument("--available-now", action="store_true")
    parser.add_argument("--bronze-ns", default="lake.bronze")
    parser.add_argument("--silver-ns", default="lake.silver")
    parser.add_argument("--topics", help="JSON object overriding topic names (tests)")
    parser.add_argument("--checkpoint-root", type=Path, default=Path("/checkpoints/stream"))
    parser.add_argument("--max-offsets-per-trigger", type=int)
    parser.add_argument("--max-files-per-batch", type=int)
    parser.add_argument("--fail-on-data-loss", action="store_true")
    parser.add_argument("--metrics", type=Path, help="JSONL file for progress records")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    cfg = StreamConfig(
        bronze_ns=args.bronze_ns,
        silver_ns=args.silver_ns,
        topics=Topics(**json.loads(args.topics)) if args.topics else Topics(),
        checkpoint_root=args.checkpoint_root,
        trigger=args.trigger,
        fail_on_data_loss=args.fail_on_data_loss,
        max_offsets_per_trigger=args.max_offsets_per_trigger,
        max_files_per_batch=args.max_files_per_batch,
    )
    spark = build_spark("ghostbus-stream", LakeSettings.from_env(dict(os.environ)))
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(
        MetricsListener(args.metrics or cfg.checkpoint_root / "metrics.jsonl")
    )
    bootstrap = bootstrap_from_env(os.environ)
    if args.available_now:
        run_available_now(spark, cfg, bootstrap)
    else:
        run_continuous(spark, cfg, bootstrap)
    return 0


if __name__ == "__main__":
    sys.exit(main())
