"""Nightly prediction accuracy: real-time countdowns vs when buses actually arrived (roadmap 1.4,
ADR-0016).

For every fully collected bus trip that ran on the day: its stops (timetable), every archived
TripUpdates snapshot's predictions, and its positions, assembled per trip in Spark; the per-trip
work (passage times, the board's prediction, errors) runs in ``ghostbus_core.accuracy`` on the
executors, and only small mergeable summaries come back. The day's summaries replace its rows in
``gold.prediction_accuracy``; the last 28 days are published as ``data/v1/accuracy.json``.

    docker compose -f local/docker-compose.yml run --rm -e EXPORT_URI=file:///tmp/site/ spark \\
        python -m ghostbus_jobs.accuracy --from 2026-11-18 --to 2026-11-18
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from collections.abc import Callable, Iterable, Iterator
from datetime import UTC, date, datetime, timedelta
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ghostbus_core.accuracy import (
    Acc,
    Fix,
    Snapshot,
    StopTime,
    Update,
    accumulate,
    accuracy_doc,
    trip_errors,
)
from ghostbus_core.lake import LakeSettings
from ghostbus_core.timetable import LOCAL_TZ
from ghostbus_ingest.archive import Archive, archive_from_uri
from ghostbus_jobs.export import publish
from ghostbus_jobs.outcome_report import AGENCY
from ghostbus_jobs.outcomes import GOLD, Sources, attach_instance, sources_from_archive
from ghostbus_jobs.quality import in_rates
from ghostbus_jobs.reconcile import data_window
from ghostbus_jobs.spark import build_spark
from ghostbus_jobs.timetable_load import DIM, Dim

log = logging.getLogger("ghostbus.accuracy")
ACCURACY = "lake.gold.prediction_accuracy"
WINDOW_DAYS = 28
SCHEMA = (
    "service_date DATE, agency STRING, lead_min STRING, n BIGINT, trips BIGINT, sum_err BIGINT, "
    "sum_abs BIGINT, within_60 BIGINT, within_120 BIGINT, tt_sum_abs BIGINT, "
    "tt_within_120 BIGINT, hist ARRAY<BIGINT>, tt_hist ARRAY<BIGINT>, stops_timed BIGINT, "
    "stops_total BIGINT, computed_at TIMESTAMP"
)
COVERAGE = "__coverage__"


def trips_frame(
    spark: SparkSession, src: Sources, day: date, gold: str = GOLD, dim: Dim = DIM
) -> DataFrame:
    """One row per trip that ran: its stops, its snapshots' updates and its positions."""
    ran = (
        spark.table(gold)
        .where(F.col("service_date") == F.lit(day))
        .where(in_rates())
        .where(F.col("outcome") == "RAN")
        .select("trip_instance_key", "agency_id", "scheduled_start")
    )
    inst = ran.join(
        spark.table(dim.trip_instance)
        .where(F.col("service_date") == F.lit(day))
        .select("trip_instance_key", "feed_version", "trip_id", "first_departure_s"),
        "trip_instance_key",
    )
    origin = F.unix_timestamp("scheduled_start") - F.col("first_departure_s")
    stops = (
        inst.join(spark.table(dim.stop_time), ["feed_version", "trip_id"])
        .join(
            spark.table(dim.stop)
            .where("is_current")
            .select(
                "stop_id",
                F.col("stop_lat").cast("double").alias("lat"),
                F.col("stop_lon").cast("double").alias("lon"),
            ),
            "stop_id",
        )
        .where(F.col("arrival_s").isNotNull())
        .groupBy("trip_instance_key", "agency_id")
        .agg(
            F.collect_list(
                F.struct(
                    F.col("stop_sequence").alias("seq"),
                    "lat",
                    "lon",
                    (origin + F.col("arrival_s")).alias("scheduled"),
                )
            ).alias("stops")
        )
    )
    keys = ran.select("trip_instance_key")
    updates = (
        attach_instance(src.trip_updates, spark, dim)
        .join(keys, "trip_instance_key")
        .where(F.size("stop_time_updates") > 0)
        .groupBy("trip_instance_key")
        .agg(
            F.collect_list(
                F.struct(F.unix_timestamp("fetched_at").alias("fetched"), "stop_time_updates")
            ).alias("snapshots")
        )
    )
    positions = (
        attach_instance(src.vehicle_positions, spark, dim)
        .join(keys, "trip_instance_key")
        .where(F.col("latitude").isNotNull())
        .groupBy("trip_instance_key")
        .agg(
            F.collect_list(
                F.struct(
                    F.unix_timestamp("event_time").alias("t"),
                    F.col("latitude").alias("lat"),
                    F.col("longitude").alias("lon"),
                )
            ).alias("fixes")
        )
    )
    return stops.join(updates, "trip_instance_key").join(positions, "trip_instance_key")


def _event(e: Any) -> tuple[int | None, int | None]:
    if e is None:
        return None, None
    return e["delay"], e["time"]


def _snapshot(row: Any) -> Snapshot:
    updates = []
    for u in row["stop_time_updates"] or []:
        if u["stop_sequence"] is None:
            continue
        delay, time = _event(u["arrival"])
        if delay is None and time is None:
            delay, time = _event(u["departure"])
        updates.append(
            Update(int(u["stop_sequence"]), delay, time, u["schedule_relationship"] == "SKIPPED")
        )
    return Snapshot(int(row["fetched"]), tuple(updates))


def summarise(rows: Iterable[Any]) -> Iterator[tuple[tuple[str, str], Acc]]:
    """Executor side: each trip through the core, summaries merged per partition."""
    out: dict[tuple[str, str], Acc] = {}
    coverage = Acc()
    for r in rows:
        stops = [
            StopTime(int(s["seq"]), float(s["lat"]), float(s["lon"]), int(s["scheduled"]))
            for s in r["stops"]
        ]
        snaps = [_snapshot(s) for s in r["snapshots"]]
        fixes = [
            Fix(int(f["t"]), float(f["lat"]), float(f["lon"]))
            for f in r["fixes"]
            if f["t"] is not None
        ]
        errors, timed, total = trip_errors(stops, snaps, fixes)
        coverage.n += timed  # stops with a passage time ...
        coverage.trips += total  # ... out of those that could have one
        agency = AGENCY.get(r["agency_id"], r["agency_id"])
        for key, acc in accumulate(errors, agency).items():
            out[key] = out[key].merge(acc) if key in out else acc
    yield (COVERAGE, ""), coverage
    yield from out.items()


def measure(
    spark: SparkSession, src: Sources, day: date, gold: str = GOLD, dim: Dim = DIM
) -> tuple[dict[tuple[str, str], Acc], tuple[int, int]]:
    parts = trips_frame(spark, src, day, gold, dim).rdd.mapPartitions(summarise).collect()
    groups: dict[tuple[str, str], Acc] = {}
    timed = total = 0
    for key, acc in parts:
        if key[0] == COVERAGE:
            timed, total = timed + acc.n, total + acc.trips
            continue
        groups[key] = groups[key].merge(acc) if key in groups else acc
    return groups, (timed, total)


def write(
    spark: SparkSession,
    day: date,
    groups: dict[tuple[str, str], Acc],
    coverage: tuple[int, int],
    table: str = ACCURACY,
) -> None:
    """Replace the day's rows: delete, then insert."""
    now = datetime.now(UTC).replace(tzinfo=None, microsecond=0)
    rows = [
        {"service_date": day, "agency": a, "lead_min": b}
        | acc.to_row()
        | {"stops_timed": coverage[0], "stops_total": coverage[1], "computed_at": now}
        for (a, b), acc in sorted(groups.items())
    ]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {table.rsplit('.', 1)[0]}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({SCHEMA}) USING iceberg")
    spark.sql(f"DELETE FROM {table} WHERE service_date = DATE '{day.isoformat()}'")
    if rows:
        spark.createDataFrame(rows, SCHEMA).writeTo(table).append()


def window(
    spark: SparkSession, end: date, table: str = ACCURACY
) -> tuple[dict[tuple[str, str], Acc], list[str], tuple[int, int]]:
    """The last 28 days' summaries, merged, for publishing."""
    start = end - timedelta(days=WINDOW_DAYS - 1)
    rows = (
        spark.table(table).where(F.col("service_date").between(F.lit(start), F.lit(end))).collect()
    )
    groups: dict[tuple[str, str], Acc] = {}
    days: dict[str, tuple[int, int]] = {}
    for r in rows:
        acc = Acc.from_row(r.asDict())
        key = (r["agency"], r["lead_min"])
        groups[key] = groups[key].merge(acc) if key in groups else acc
        days[r["service_date"].isoformat()] = (int(r["stops_timed"]), int(r["stops_total"]))
    timed = sum(t for t, _ in days.values())
    total = sum(n for _, n in days.values())
    return groups, sorted(days), (timed, total)


def run(
    spark: SparkSession,
    days: list[date],
    target: Archive | None,
    source: Callable[[date], Sources],
    gold: str = GOLD,
    table: str = ACCURACY,
    dim: Dim = DIM,
) -> dict[str, Any]:
    for day in days:
        groups, coverage = measure(spark, source(day), day, gold, dim)
        write(spark, day, groups, coverage, table)
        log.info("%s: %d groups, stops timed %d of %d", day, len(groups), *coverage)
    groups, published, coverage = window(spark, days[-1], table)
    doc = accuracy_doc(
        groups, published, coverage, datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    )
    if target is not None:
        publish(target, {"accuracy.json": doc})
    rows = {r["lead_min"]: r for r in doc["by_lead"]}
    print(
        "accuracy:",
        ", ".join(
            f"{b} min: median {r.get('median_min')} (n={r['predictions']})" for b, r in rows.items()
        ),
    )
    return doc


def main(argv: list[str] | None = None) -> int:
    yesterday = datetime.now(LOCAL_TZ).date() - timedelta(days=1)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from", dest="first", type=date.fromisoformat, default=yesterday)
    parser.add_argument("--to", dest="last", type=date.fromisoformat, default=yesterday)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    spark = build_spark("accuracy", LakeSettings.from_env(dict(os.environ)))
    spark.sparkContext.setLogLevel("ERROR")
    archive_uri, endpoint = os.environ["ARCHIVE_URI"], os.environ.get("ARCHIVE_S3_ENDPOINT")

    def source(day: date) -> Sources:
        return sources_from_archive(spark, archive_uri, endpoint, *data_window(day, day))

    days = [args.first + timedelta(days=i) for i in range((args.last - args.first).days + 1)]
    run(spark, days, archive_from_uri(os.environ["EXPORT_URI"], endpoint), source)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
