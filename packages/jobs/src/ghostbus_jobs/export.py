"""Nightly public export: the JSON files the website reads (roadmap 2.1, ADR-0011).

Runs after the quality job. Aggregates the last 28 service days of fully collected bus trips per
day, route and local hour; reads each day's quality verdict and the current stop list; builds every
document with ``ghostbus_core.export``; and uploads only the files whose bytes changed.

    docker compose -f local/docker-compose.yml run --rm -e EXPORT_URI=file:///tmp/site/ spark \\
        python -m ghostbus_jobs.export --to 2026-11-17
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
from datetime import UTC, date, datetime, timedelta
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ghostbus_core.export import Cell, Stop, build, dumps, publishable
from ghostbus_core.lake import LakeSettings
from ghostbus_core.model import fit
from ghostbus_core.outcomes import DEFAULT
from ghostbus_core.provenance import Seal
from ghostbus_core.timetable import LOCAL_TZ
from ghostbus_ingest.archive import Archive, archive_from_env, archive_from_uri
from ghostbus_jobs.outcome_report import AGENCY
from ghostbus_jobs.outcomes import GOLD
from ghostbus_jobs.quality import QUALITY, in_rates
from ghostbus_jobs.spark import build_spark
from ghostbus_jobs.timetable_load import DIM, Dim

log = logging.getLogger("ghostbus.export")
WINDOW_DAYS = 28


def gather_cells(spark: SparkSession, end: date, gold: str = GOLD) -> list[Cell]:
    start = end - timedelta(days=WINDOW_DAYS - 1)
    local = F.from_utc_timestamp("scheduled_start", "Europe/Dublin")
    # A cancellation notice before the trip was due (heuristic-v2 learns how often these still ran)
    notice = F.col("cancelled_at").isNotNull() & (F.col("cancelled_at") <= F.col("scheduled_start"))
    rows = (
        spark.table(gold)
        .where(F.col("service_date").between(F.lit(start), F.lit(end)))
        .where(in_rates())
        .groupBy(
            "service_date",
            "agency_id",
            F.split("route_key", r"\|")[1].alias("route"),
            F.hour(local).alias("hour"),
        )
        .agg(
            F.count("*").alias("n"),
            *[
                F.sum((F.col("outcome") == o).cast("int")).alias(name)
                for name, o in {
                    "ran": "RAN",
                    "ghost": "GHOST",
                    "phantom": "PHANTOM_LIVE",
                    "vanished": "VANISHED",
                    "cancelled_early": "CANCELLED_EARLY",
                    "cancelled_late": "CANCELLED_LATE",
                }.items()
            ],
            F.sum(notice.cast("int")).alias("notice"),
            F.sum((notice & (F.col("outcome") == "RAN")).cast("int")).alias("notice_ran"),
        )
        .collect()
    )
    return [
        Cell(
            service_date=r["service_date"],
            agency=AGENCY.get(r["agency_id"], r["agency_id"]),
            route=r["route"],
            hour=int(r["hour"]),
            n=int(r["n"]),
            ran=int(r["ran"]),
            ghost=int(r["ghost"]),
            phantom=int(r["phantom"]),
            vanished=int(r["vanished"]),
            cancelled_early=int(r["cancelled_early"]),
            cancelled_late=int(r["cancelled_late"]),
            notice=int(r["notice"]),
            notice_ran=int(r["notice_ran"]),
        )
        for r in rows
    ]


def gather_quality(spark: SparkSession, end: date, table: str = QUALITY) -> dict[date, str]:
    if not spark.catalog.tableExists(table):
        return {}
    start = end - timedelta(days=WINDOW_DAYS - 1)
    rows = (
        spark.table(table)
        .where(F.col("service_date").between(F.lit(start), F.lit(end)))
        .groupBy("service_date")
        .agg(F.max("overall").alias("overall"))
        .collect()
    )
    return {r["service_date"]: r["overall"] for r in rows}


def gather_stops(spark: SparkSession, dim: Dim = DIM) -> list[Stop]:
    """Current stops with a plate code, and the bus and other routes of the newest version."""
    latest = (
        spark.table(dim.feed_version).orderBy(F.desc("valid_from")).select("feed_version").first()
    )
    if latest is None:
        return []
    fv = latest["feed_version"]
    serving = (
        spark.table(dim.stop_time)
        .where(F.col("feed_version") == fv)
        .select("trip_id", "stop_id")
        .distinct()
        .join(
            spark.table(dim.trip).where(F.col("feed_version") == fv).select("trip_id", "route_key"),
            "trip_id",
        )
        .groupBy("stop_id")
        .agg(F.collect_set(F.split("route_key", r"\|")[1]).alias("routes"))
    )
    rows = (
        spark.table(dim.stop)
        .where("is_current AND stop_code IS NOT NULL AND stop_code <> ''")
        .select("stop_id", "stop_code", "stop_name", "stop_lat", "stop_lon")
        .join(serving, "stop_id")
        .collect()
    )
    return [
        Stop(
            r["stop_code"],
            r["stop_name"],
            float(r["stop_lat"]),
            float(r["stop_lon"]),
            tuple(r["routes"]),
        )
        for r in rows
    ]


def chain_head(archive: Archive) -> str | None:
    keys = sorted(k for k in archive.list_keys("provenance/") if k.endswith(".json"))
    if not keys:
        return None
    raw = archive.get_bytes(keys[-1])
    return Seal.from_json(json.loads(raw)).chain if raw else None


def publish(target: Archive, docs: dict[str, dict[str, Any]]) -> tuple[int, int]:
    """Upload documents whose bytes differ from what is there. Returns (written, unchanged)."""
    existing = {key: etag for key, _, etag in target.list_objects("")}
    written = unchanged = 0
    for key, doc in sorted(docs.items()):
        body = dumps(doc)
        if existing.get(key) == hashlib.md5(body, usedforsecurity=False).hexdigest():
            unchanged += 1
            continue
        target.put_bytes(key, body, "application/json")
        written += 1
    return written, unchanged


def run(
    spark: SparkSession,
    end: date,
    target: Archive,
    archive: Archive | None,
    gold: str = GOLD,
    quality_table: str = QUALITY,
    dim: Dim = DIM,
) -> dict[str, dict[str, Any]]:
    cells = gather_cells(spark, end, gold)
    quality = gather_quality(spark, end, quality_table)
    # The probability model (heuristic-v2), fitted on the published days only, like every rate.
    published = publishable(quality)
    model_doc = fit([c for c in cells if c.service_date in published]).to_doc()
    docs = build(
        cells=cells,
        quality=quality,
        stops=gather_stops(spark, dim),
        end=end,
        generated_at=datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        params_version=DEFAULT.version,
        chain_head=chain_head(archive) if archive else None,
        extra={"model.json": model_doc},
    )
    written, unchanged = publish(target, docs)
    log.info("export to %s: %d files written, %d unchanged", target.uri, written, unchanged)
    print(
        f"export: {len(docs)} documents, {written} written, {unchanged} unchanged -> {target.uri}"
    )
    return docs


def main(argv: list[str] | None = None) -> int:
    yesterday = datetime.now(LOCAL_TZ).date() - timedelta(days=1)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--to", dest="end", type=date.fromisoformat, default=yesterday)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    spark = build_spark("export", LakeSettings.from_env(dict(os.environ)))
    spark.sparkContext.setLogLevel("ERROR")
    target = archive_from_uri(os.environ["EXPORT_URI"], os.environ.get("ARCHIVE_S3_ENDPOINT"))
    run(spark, args.end, target, archive_from_env(os.environ))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
