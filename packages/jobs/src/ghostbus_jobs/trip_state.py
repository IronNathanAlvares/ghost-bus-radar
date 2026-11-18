"""trip_state: provisional outcomes from the live stream, a few minutes after each trip closes.

A streaming query on ``silver.heartbeats`` (one row a minute) whose ``foreachBatch`` decides every
trip that has closed since the last decision, from the silver tables, with the shared classifier,
and ``MERGE``s the result into ``gold.trip_outcomes`` as ``provisional``.

Idempotent by design rather than by bookkeeping: the clock is the data's own (the newest
heartbeat minus the watermark), never the wall clock, so replaying a micro-batch recomputes the
same rows, and the ``MERGE`` leaves identical rows untouched. Reconcile later writes ``final``
outcomes, which this job never overwrites.

    python -m ghostbus_jobs.trip_state            # runs until stopped, 60 s trigger
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import UTC, timedelta
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ghostbus_core.lake import LakeSettings
from ghostbus_core.outcomes import DEFAULT
from ghostbus_jobs.outcomes import GOLD, merge_outcomes, outcomes, sources_from_silver
from ghostbus_jobs.spark import build_spark
from ghostbus_jobs.stream import StreamConfig, iceberg_stream

log = logging.getLogger("ghostbus.trip_state")

#: Silver is complete up to the newest event minus the watermark.
WATERMARK = timedelta(minutes=10)
#: How far back each micro-batch looks for closed, undecided trips (covers restarts).
LOOKBACK = timedelta(hours=6)


def decide(spark: SparkSession, cfg: StreamConfig, table: str = GOLD) -> int:
    newest = spark.table(cfg.silver("heartbeats")).agg(F.max("fetched_at")).first()
    if not newest or newest[0] is None:
        return 0
    clock = newest[0].replace(tzinfo=UTC) - WATERMARK
    evidence_from = clock - LOOKBACK - timedelta(hours=4)  # long trips + closing time
    src = sources_from_silver(spark, cfg, evidence_from, clock)
    df = outcomes(
        spark,
        src,
        (clock - LOOKBACK - timedelta(days=1)).date(),
        clock.date(),
        clock,
        "provisional",
        DEFAULT,
        horizon_start=evidence_from,
        closed_after=clock - LOOKBACK,
    ).cache()
    n = df.count()
    if n:
        merge_outcomes(spark, df, table)
    return n


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trigger", default="60 seconds")
    parser.add_argument("--checkpoint", type=Path, default=Path("/checkpoints/trip_state"))
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    spark = build_spark("trip_state", LakeSettings.from_env(dict(os.environ)))
    spark.sparkContext.setLogLevel("WARN")
    cfg = StreamConfig()

    def on_batch(_batch: DataFrame, batch_id: int) -> None:
        n = decide(spark, cfg)
        log.info("batch %d: %d trips decided (provisional)", batch_id, n)

    (
        iceberg_stream(spark, cfg.silver("heartbeats"))
        .writeStream.queryName("trip_state")
        .foreachBatch(on_batch)
        .option("checkpointLocation", str(args.checkpoint))
        .trigger(processingTime=args.trigger)
        .start()
        .awaitTermination()
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
