"""reconcile: final trip outcomes from the raw archive. Backfill is this job over a date range.

The archive is written by the collector whether or not Kafka and the stream are up (ADR-0001),
so reconcile repairs any stream outage and also picks up the late rows the stream's watermark
dropped. It writes ``status = 'final'``, which provisional stream outcomes never overwrite.

    docker compose -f local/docker-compose.yml run --rm spark \\
        python -m ghostbus_jobs.reconcile --from 2026-11-15 --to 2026-11-15

Only trips closed by the data clock (the newest heartbeat in the archive window) are decided;
run it again later and the rest of the day fills in. Reruns are idempotent.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import UTC, date, datetime, time, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ghostbus_core.lake import LakeSettings
from ghostbus_core.outcomes import DEFAULT, Params
from ghostbus_core.timetable import LOCAL_TZ
from ghostbus_jobs.outcomes import (
    GOLD,
    Sources,
    merge_outcomes,
    outcomes,
    sources_from_archive,
    sources_from_silver,
)
from ghostbus_jobs.spark import build_spark
from ghostbus_jobs.stream import StreamConfig

log = logging.getLogger("ghostbus.reconcile")


def data_window(first: date, last: date) -> tuple[datetime, datetime]:
    """Evidence needed for service days first..last: from 2 h before the first day's local
    midnight to 6 h into the morning after the last (trips after midnight, plus closing time)."""
    lo = datetime.combine(first, time(0), tzinfo=LOCAL_TZ) - timedelta(hours=2)
    hi = datetime.combine(last + timedelta(days=1), time(6), tzinfo=LOCAL_TZ)
    return lo.astimezone(UTC), min(hi.astimezone(UTC), datetime.now(UTC))


def data_clock(src: Sources) -> datetime | None:
    row = src.heartbeats.agg(F.max("fetched_at")).first()
    return row[0].replace(tzinfo=UTC) if row and row[0] else None


def reconcile(
    spark: SparkSession,
    first: date,
    last: date,
    source: str = "archive",
    p: Params = DEFAULT,
    table: str = GOLD,
) -> int:
    lo, hi = data_window(first, last)
    if source == "archive":
        src = sources_from_archive(
            spark, os.environ["ARCHIVE_URI"], os.environ.get("ARCHIVE_S3_ENDPOINT"), lo, hi
        )
    else:
        src = sources_from_silver(spark, StreamConfig(), lo, hi)
    clock = data_clock(src)
    if clock is None:
        log.warning("no collected data between %s and %s", lo, hi)
        return 0
    df = outcomes(spark, src, first, last, clock, "final", p, horizon_start=lo).cache()
    n = df.count()
    merge_outcomes(spark, df, table)
    log.info("decided %d trips for %s..%s from %s (data clock %s)", n, first, last, source, clock)
    return n


def main(argv: list[str] | None = None) -> int:
    yesterday = datetime.now(LOCAL_TZ).date() - timedelta(days=1)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from", dest="first", type=date.fromisoformat, default=yesterday)
    parser.add_argument("--to", dest="last", type=date.fromisoformat, default=yesterday)
    parser.add_argument("--source", choices=["archive", "silver"], default="archive")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    spark = build_spark("reconcile", LakeSettings.from_env(dict(os.environ)))
    spark.sparkContext.setLogLevel("ERROR")
    n = reconcile(spark, args.first, args.last, args.source)
    print(f"final outcomes written: {n:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
