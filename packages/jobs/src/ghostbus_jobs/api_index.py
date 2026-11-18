"""Nightly stop index for the "will my bus come?" API (roadmap 2.3, ADR-0012).

Runs after ``timetable_load``. Every bus departure at every stop for today and tomorrow (local
service days), from ``dim.trip_instance`` (which already knows which timetable version defines
each day), written as 256 gzipped shards under a new build prefix, then the pointer
``current.json`` last, so the API switches from one complete build to the next.

    docker compose -f local/docker-compose.yml run --rm -e API_INDEX_URI=file:///tmp/api/ spark \\
        python -m ghostbus_jobs.api_index --date 2026-11-19
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ghostbus_core.departures import Departure, StopInfo, Trip, index_docs
from ghostbus_core.export import slug
from ghostbus_core.lake import LakeSettings
from ghostbus_core.timetable import LOCAL_TZ, SCHEDULE_CUTOFF_LOCAL
from ghostbus_ingest.archive import Archive, archive_from_uri
from ghostbus_jobs.outcome_report import AGENCY
from ghostbus_jobs.spark import build_spark
from ghostbus_jobs.timetable_load import DIM, Dim, gtfs_time_text

log = logging.getLogger("ghostbus.api_index")
DAYS = 2
BUS = "3"


def departures_frame(
    spark: SparkSession, first: date, days: int = DAYS, dim: Dim = DIM
) -> DataFrame:
    """One row per bus departure a passenger can board: not the last stop, pickup allowed."""
    last = first + timedelta(days=days - 1)
    instances = (
        spark.table(dim.trip_instance)
        .where(F.col("service_date").between(F.lit(first), F.lit(last)))
        .where(F.split("route_key", r"\|")[2] == BUS)
        .select(
            "service_date",
            "feed_version",
            "trip_id",
            "route_key",
            "direction_id",
            "first_departure_s",
            F.unix_timestamp("scheduled_start").alias("start_at"),
        )
    )
    trips = spark.table(dim.trip).select("feed_version", "trip_id", "route_id", "trip_headsign")
    stop_times = (
        spark.table(dim.stop_time)
        .where(F.col("departure_s").isNotNull())
        .where(F.coalesce(F.col("pickup_type"), F.lit("0")) != "1")
        .select("feed_version", "trip_id", "stop_sequence", "stop_id", "departure_s")
    )
    last_stop = (
        spark.table(dim.stop_time)
        .groupBy("feed_version", "trip_id")
        .agg(F.max("stop_sequence").alias("last_sequence"))
    )
    stops = (
        spark.table(dim.stop)
        .where("is_current AND stop_code IS NOT NULL AND stop_code <> ''")
        .select("stop_id", "stop_code")
    )
    return (
        instances.join(trips, ["feed_version", "trip_id"])
        .join(stop_times, ["feed_version", "trip_id"])
        .join(last_stop, ["feed_version", "trip_id"])
        .where(F.col("stop_sequence") < F.col("last_sequence"))
        .join(stops, "stop_id")
        .select(
            "stop_code",
            (F.col("start_at") + F.col("departure_s") - F.col("first_departure_s")).alias("at"),
            "stop_sequence",
            "trip_id",
            "route_id",
            "direction_id",
            F.date_format("service_date", "yyyyMMdd").alias("start_date"),
            gtfs_time_text(F.col("first_departure_s")).alias("start_time"),
            "start_at",
            F.split("route_key", r"\|")[0].alias("agency_id"),
            F.split("route_key", r"\|")[1].alias("route"),
            F.coalesce(F.col("trip_headsign"), F.lit("")).alias("headsign"),
        )
    )


def gather(
    spark: SparkSession, first: date, dim: Dim = DIM
) -> tuple[list[StopInfo], list[Departure]]:
    stops = [
        StopInfo(r["stop_code"], r["stop_name"], float(r["stop_lat"]), float(r["stop_lon"]))
        for r in spark.table(dim.stop)
        .where("is_current AND stop_code IS NOT NULL AND stop_code <> ''")
        .groupBy("stop_code")
        .agg(
            F.min("stop_name").alias("stop_name"),
            F.min("stop_lat").alias("stop_lat"),
            F.min("stop_lon").alias("stop_lon"),
        )
        .collect()
    ]
    trips: dict[tuple[str, str], Trip] = {}
    departures: list[Departure] = []
    rows: Iterator = departures_frame(spark, first, dim=dim).toLocalIterator()  # type: ignore[type-arg]
    for r in rows:
        key = (r["trip_id"], r["start_date"])
        trip = trips.get(key)
        if trip is None:
            agency = AGENCY.get(r["agency_id"], r["agency_id"])
            trip = Trip(
                r["trip_id"],
                r["route_id"] or "",
                r["direction_id"] or "",
                r["start_date"],
                r["start_time"],
                int(r["start_at"]),
                agency,
                r["route"],
                r["headsign"],
                slug(agency, r["route"]),
            )
            trips[key] = trip
        departures.append(Departure(r["stop_code"], int(r["at"]), int(r["stop_sequence"]), trip))
    return stops, departures


def feed_versions(spark: SparkSession, first: date, dim: Dim = DIM) -> list[str]:
    last = first + timedelta(days=DAYS - 1)
    return [
        r["feed_version"]
        for r in spark.table(dim.trip_instance)
        .where(F.col("service_date").between(F.lit(first), F.lit(last)))
        .select("feed_version")
        .distinct()
        .collect()
    ]


def publish(target: Archive, docs: dict[str, bytes]) -> None:
    """Shards first, the pointer last: a reader sees the old build or the new one, never half."""
    for key, body in docs.items():
        ctype = "application/json" if key.endswith(".json") else "application/gzip"
        target.put_bytes(key, body, ctype)


def run(
    spark: SparkSession, first: date, target: Archive, build: str | None = None, dim: Dim = DIM
) -> dict[str, bytes]:
    build = build or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    stops, departures = gather(spark, first, dim)
    # Covered: service days first .. first+DAYS-1. The next one starts at its 04:00 local cutoff,
    # a full day after the next nightly run would have replaced this build.
    until = datetime.combine(first + timedelta(days=DAYS), SCHEDULE_CUTOFF_LOCAL, tzinfo=LOCAL_TZ)
    docs = index_docs(
        stops, departures, build, feed_versions(spark, first, dim), int(until.timestamp())
    )
    publish(target, docs)
    size = sum(len(b) for b in docs.values())
    print(
        f"api index {build}: {len(departures):,} departures at {len(stops):,} stops, "
        f"{len(docs)} files, {size / 1e6:.1f} MB -> {target.uri}"
    )
    return docs


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    today = datetime.now(LOCAL_TZ).date()
    parser.add_argument("--date", dest="first", type=date.fromisoformat, default=today)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    spark = build_spark("api_index", LakeSettings.from_env(dict(os.environ)))
    spark.sparkContext.setLogLevel("ERROR")
    target = archive_from_uri(os.environ["API_INDEX_URI"], os.environ.get("ARCHIVE_S3_ENDPOINT"))
    run(spark, args.first, target)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
