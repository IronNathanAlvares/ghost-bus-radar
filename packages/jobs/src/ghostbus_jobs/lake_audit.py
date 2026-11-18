"""Audit: every fetch in the lake must exist in the raw archive.

The archive is the record of what the collector actually fetched (ADR-0001). A ``fetch_id`` in
bronze or silver that the archive does not have came from somewhere else: in Sprint 4, an
integration test that had published its fixture to the live topics. Such rows are worse than
noise when they are heartbeats, because a fake "collected" heartbeat can hide a real gap.

    docker compose -f local/docker-compose.yml run --rm spark python -m ghostbus_jobs.lake_audit
    ... python -m ghostbus_jobs.lake_audit --delete     # remove them from every layer

Deleting is safe with the streams running: their readers skip correction snapshots
(``stream.SKIP_CORRECTIONS``), and a commit that conflicts with a concurrent append is retried.
"""

from __future__ import annotations

import argparse
import os
import sys
import time

from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession

from ghostbus_core.archive_layout import fetch_id, parse_heartbeat_key, parse_snapshot_key
from ghostbus_core.lake import LakeSettings
from ghostbus_ingest.archive import Archive, archive_from_uri
from ghostbus_jobs.spark import build_spark
from ghostbus_jobs.stream import StreamConfig

SILVER = ("trip_updates", "vehicle_positions", "heartbeats", "stop_time_predictions")


def archived_fetch_ids(archive: Archive) -> set[str]:
    out = set()
    for key in archive.list_keys("raw/"):
        for parse in (parse_snapshot_key, parse_heartbeat_key):
            try:
                feed, ms = parse(key)
            except ValueError:
                continue
            out.add(fetch_id(feed, ms))
            break
    return out


def lake_tables(cfg: StreamConfig) -> list[str]:
    return [cfg.bronze, *(cfg.silver(n) for n in SILVER)]


def unknown_fetches(spark: SparkSession, tables: list[str], known: set[str]) -> dict[str, set[str]]:
    out = {}
    for t in tables:
        ids = {r[0] for r in spark.table(t).select("fetch_id").distinct().collect() if r[0]}
        out[t] = ids - known
    return out


def delete_fetches(spark: SparkSession, table: str, ids: set[str], attempts: int = 5) -> int:
    in_list = ", ".join(f"'{i}'" for i in sorted(ids))
    n = spark.table(table).where(f"fetch_id IN ({in_list})").count()
    for attempt in range(1, attempts + 1):
        try:
            spark.sql(f"DELETE FROM {table} WHERE fetch_id IN ({in_list})")
            return n
        except Py4JJavaError as e:  # a concurrent stream append conflicted: idempotent, retry
            if attempt == attempts or "ValidationException" not in str(e.java_exception):
                raise
            time.sleep(5 * attempt)
    return n


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--delete", action="store_true", help="remove unknown fetches")
    args = parser.parse_args(argv)
    archive = archive_from_uri(os.environ["ARCHIVE_URI"], os.environ.get("ARCHIVE_S3_ENDPOINT"))
    known = archived_fetch_ids(archive)
    spark = build_spark("lake_audit", LakeSettings.from_env(dict(os.environ)))
    spark.sparkContext.setLogLevel("ERROR")
    tables = lake_tables(StreamConfig())
    unknown = unknown_fetches(spark, tables, known)
    print(f"Fetches in the archive: {len(known):,}\n")
    print("| Table | Fetches not in the archive |\n|---|---|")
    for t, ids in unknown.items():
        print(f"| {t} | {len(ids)} |")
    bad = set().union(*unknown.values())
    for i in sorted(bad)[:20]:
        print(f"- `{i}`")
    if args.delete and bad:
        print()
        for t in tables:  # every layer, so no stream re-derives what was removed upstream
            print(f"deleted {delete_fetches(spark, t, bad):,} rows from {t}")
        bad = set().union(*unknown_fetches(spark, tables, known).values())
    spark.stop()
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
