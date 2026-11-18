"""End-to-end smoke test and latency probe: Kafka -> Spark Structured Streaming -> Iceberg.

Produces ``n`` events, one per ``interval`` seconds, while a streaming query writes them to
``lake.smoke.events`` through Iceberg's streaming sink. Latency for each event is measured from
the moment the producer created it to the commit time of the Iceberg snapshot that made it
queryable, using the table's own ``entries`` and ``snapshots`` metadata tables.

Run inside the local stack::

    docker compose -f local/docker-compose.yml run --rm spark python -m ghostbus_jobs.smoke
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
import uuid
from dataclasses import asdict, dataclass

from confluent_kafka import Consumer, Producer, TopicPartition
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from ghostbus_core.lake import LakeSettings, table_properties
from ghostbus_core.topics import SMOKE
from ghostbus_ingest.kafka_admin import ensure_topics
from ghostbus_ingest.kafka_config import bootstrap_from_env, producer_config
from ghostbus_jobs.spark import build_spark

TABLE = "lake.smoke.events"
PAYLOAD = StructType(
    [
        StructField("run_id", StringType(), nullable=False),
        StructField("event_id", IntegerType(), nullable=False),
        StructField("produced_at_ms", LongType(), nullable=False),
    ]
)


@dataclass(frozen=True)
class SmokeResult:
    run_id: str
    events_sent: int
    events_landed: int
    latency_ms_p50: float
    latency_ms_p95: float
    latency_ms_max: float
    trigger: str


def _percentile(values: list[float], pct: float) -> float:
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, round(pct / 100 * (len(ordered) - 1))))
    return ordered[index]


def _end_offsets(bootstrap: str, topic: str) -> str:
    """Current end offset of every partition, as Spark's ``startingOffsets`` JSON.

    Starting exactly here (not at "latest") avoids a race where events produced before the
    query resolves "latest" would be skipped, and avoids replaying older smoke runs.
    """
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": f"smoke-probe-{uuid.uuid4()}"})
    try:
        partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
        offsets = {
            str(p): consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1]
            for p in partitions
        }
    finally:
        consumer.close()
    return json.dumps({topic: offsets})


def _create_table(spark: SparkSession) -> None:
    props = ", ".join(f"'{k}'='{v}'" for k, v in table_properties().items())
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.smoke")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            run_id STRING, event_id INT, produced_at TIMESTAMP,
            kafka_partition INT, kafka_offset BIGINT, kafka_timestamp TIMESTAMP
        ) USING iceberg TBLPROPERTIES ({props})
        """
    )


def _count_landed(spark: SparkSession, run_id: str) -> int:
    spark.sql(f"REFRESH TABLE {TABLE}")
    return spark.table(TABLE).where(F.col("run_id") == run_id).count()


def _latencies_ms(spark: SparkSession, run_id: str) -> list[float]:
    rows = spark.sql(
        f"""
        SELECT r.event_id,
               unix_millis(s.committed_at) - unix_millis(r.produced_at) AS latency_ms
        FROM (SELECT event_id, produced_at, _file FROM {TABLE} WHERE run_id = '{run_id}') r
        JOIN (SELECT snapshot_id, data_file.file_path AS file_path
              FROM {TABLE}.entries WHERE status = 1) e ON r._file = e.file_path
        JOIN {TABLE}.snapshots s ON s.snapshot_id = e.snapshot_id
        """
    ).collect()
    return [float(row["latency_ms"]) for row in rows]


def run(n_events: int, interval_s: float, trigger_ms: int, timeout_s: float) -> SmokeResult:
    env = dict(os.environ)
    bootstrap = bootstrap_from_env(env)
    run_id = uuid.uuid4().hex
    ensure_topics(bootstrap, replication_factor=1)

    spark = build_spark("ghostbus-smoke", LakeSettings.from_env(env))
    _create_table(spark)

    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", SMOKE.name)
        .option("startingOffsets", _end_offsets(bootstrap, SMOKE.name))
        .option("failOnDataLoss", "true")
        .load()
    )
    events = source.select(
        F.from_json(F.col("value").cast("string"), PAYLOAD).alias("v"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).select(
        "v.run_id",
        "v.event_id",
        F.timestamp_millis("v.produced_at_ms").alias("produced_at"),
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
    )
    trigger = f"{trigger_ms} milliseconds"
    query = (
        events.writeStream.format("iceberg")
        .outputMode("append")
        .trigger(processingTime=trigger)
        .option("checkpointLocation", f"/tmp/ghostbus-checkpoints/smoke/{run_id}")
        .toTable(TABLE)
    )

    producer = Producer(producer_config(bootstrap, client_id="ghostbus-smoke"))
    try:
        for event_id in range(n_events):
            payload = {
                "run_id": run_id,
                "event_id": event_id,
                "produced_at_ms": time.time_ns() // 1_000_000,
            }
            producer.produce(SMOKE.name, key=run_id, value=json.dumps(payload).encode())
            producer.flush(10)
            time.sleep(interval_s)

        deadline = time.monotonic() + timeout_s
        landed = _count_landed(spark, run_id)
        while landed < n_events and time.monotonic() < deadline:
            if query.exception() is not None:
                raise RuntimeError(f"streaming query failed: {query.exception()}")
            time.sleep(0.5)
            landed = _count_landed(spark, run_id)
    finally:
        query.stop()

    latencies = _latencies_ms(spark, run_id) or [float("nan")]
    spark.stop()
    return SmokeResult(
        run_id=run_id,
        events_sent=n_events,
        events_landed=landed,
        latency_ms_p50=statistics.median(latencies),
        latency_ms_p95=_percentile(latencies, 95),
        latency_ms_max=max(latencies),
        trigger=trigger,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--events", type=int, default=20)
    parser.add_argument("--interval", type=float, default=1.0, help="seconds between events")
    parser.add_argument("--trigger-ms", type=int, default=500)
    parser.add_argument("--timeout", type=float, default=120.0)
    args = parser.parse_args(argv)
    result = run(args.events, args.interval, args.trigger_ms, args.timeout)
    print(json.dumps(asdict(result), indent=2))
    return 0 if result.events_landed == result.events_sent else 1


if __name__ == "__main__":
    sys.exit(main())
