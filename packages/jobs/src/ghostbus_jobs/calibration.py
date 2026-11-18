"""Nightly calibration backtest for "will my bus come?" (roadmap 2.4, ADR-0014).

For a test day, every fully collected bus trip is asked about at 30, 15, 5 and 1 minutes before
its scheduled start: the archived live snapshots of that moment are replayed through the
production matcher to get the status the API would have shown, and the production model gives
the probability, using route history from the 28 days *before* the test day only. Predictions
replace that day's rows in ``gold.calibration_predictions``; the last 28 days are then scored and
published as ``data/v1/calibration.json``.

    docker compose -f local/docker-compose.yml run --rm -e EXPORT_URI=file:///tmp/site/ spark \\
        python -m ghostbus_jobs.calibration --from 2026-11-16 --to 2026-11-17
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ghostbus_core.calibration import (
    LEADS_MIN,
    WINDOW_DAYS,
    Prediction,
    calibration_doc,
    live_status,
)
from ghostbus_core.departures import (
    MAX_LIVE_AGE_S,
    MODEL,
    Departure,
    RouteHistory,
    Trip,
    history_from_route_doc,
    probability,
)
from ghostbus_core.export import route_doc, slug
from ghostbus_core.lake import LakeSettings
from ghostbus_core.timetable import LOCAL_TZ
from ghostbus_ingest.archive import Archive, archive_from_uri
from ghostbus_jobs.export import gather_cells, publish
from ghostbus_jobs.outcome_report import AGENCY
from ghostbus_jobs.outcomes import GOLD, Sources, attach_instance, sources_from_archive
from ghostbus_jobs.quality import in_rates
from ghostbus_jobs.reconcile import data_window
from ghostbus_jobs.spark import build_spark
from ghostbus_jobs.timetable_load import DIM, Dim

log = logging.getLogger("ghostbus.calibration")
PREDICTIONS = "lake.gold.calibration_predictions"
SCHEMA = (
    "service_date DATE, trip_instance_key STRING, lead_min INT, predicted_at TIMESTAMP, "
    "route_slug STRING, status STRING, probability DOUBLE, ran INT, outcome STRING, "
    "base_rate DOUBLE, model STRING, computed_at TIMESTAMP"
)


# --- history (training days only) ---------------------------------------------------------------


def histories(
    spark: SparkSession, day: date, gold: str = GOLD
) -> tuple[dict[str, RouteHistory], float | None]:
    """Route histories from the 28 days before ``day``, through the same path as the published
    route files; plus the base rate (share of trips that ran) over the same window."""
    cells = gather_cells(spark, day - timedelta(days=1), gold)
    days = {c.service_date: "ok" for c in cells}
    out = {}
    for agency, route in sorted({(c.agency, c.route) for c in cells}):
        doc = route_doc(agency, route, cells, days, day - timedelta(days=1))
        out[slug(agency, route)] = history_from_route_doc(doc)
    n = sum(c.n for c in cells)
    return out, (sum(c.ran for c in cells) / n if n else None)


# --- the live status at each lead time ----------------------------------------------------------


def test_trips(spark: SparkSession, day: date, gold: str = GOLD) -> DataFrame:
    return (
        spark.table(gold)
        .where(F.col("service_date") == F.lit(day))
        .where(in_rates())
        .select(
            "service_date",
            "trip_instance_key",
            "agency_id",
            "route_key",
            "scheduled_start",
            "outcome",
        )
    )


def moments(trips: DataFrame) -> DataFrame:
    """One row per trip and lead time: ``asked_at`` = scheduled start minus the lead."""
    leads = trips.sparkSession.createDataFrame([(m,) for m in LEADS_MIN], "lead_min INT")
    return trips.crossJoin(leads).withColumn(
        "asked_at", F.col("scheduled_start") - F.expr("make_interval(0, 0, 0, 0, 0, lead_min, 0)")
    )


def _newest_fetch(points: DataFrame, fetches: DataFrame, feed: str) -> DataFrame:
    """For each moment, the newest collected snapshot of ``feed`` within 15 minutes before it."""
    f = fetches.where(F.col("feed") == feed).select(F.col("fetched_at").alias("_f"))
    return (
        points.select("trip_instance_key", "lead_min", "asked_at")
        .join(
            F.broadcast(f),
            (F.col("_f") <= F.col("asked_at"))
            & (F.col("_f") > F.col("asked_at") - F.expr(f"INTERVAL {MAX_LIVE_AGE_S} SECONDS")),
        )
        .groupBy("trip_instance_key", "lead_min")
        .agg(F.max("_f").alias(f"{feed}_at"))
    )


def statuses(spark: SparkSession, src: Sources, points: DataFrame, dim: Dim = DIM) -> DataFrame:
    """Per trip and lead: the newest snapshots' age, and cancelled / tracked / assigned in them."""
    fetches = src.heartbeats.where(F.col("collected")).select("feed", "fetched_at").distinct()
    keys = ["trip_instance_key", "lead_min"]
    tu_at = _newest_fetch(points, fetches, "trip_updates")
    vp_at = _newest_fetch(points, fetches, "vehicles")
    tu = attach_instance(src.trip_updates, spark, dim).select(
        "trip_instance_key",
        F.col("fetched_at").alias("trip_updates_at"),
        (F.col("schedule_relationship") == "CANCELED").alias("cancelled"),
        F.col("vehicle_id").isNotNull().alias("assigned"),
    )
    vp = attach_instance(src.vehicle_positions, spark, dim).select(
        "trip_instance_key", F.col("fetched_at").alias("vehicles_at"), F.lit(True).alias("tracked")
    )
    tu_hit = (
        tu_at.join(tu, ["trip_instance_key", "trip_updates_at"])
        .groupBy(*keys)
        .agg(F.max("cancelled").alias("cancelled"), F.max("assigned").alias("assigned"))
    )
    vp_hit = (
        vp_at.join(vp, ["trip_instance_key", "vehicles_at"])
        .groupBy(*keys)
        .agg(F.max("tracked").alias("tracked"))
    )
    return (
        points.join(tu_at, keys, "left")
        .join(tu_hit, keys, "left")
        .join(vp_hit, keys, "left")
        .withColumn(
            "snapshot_age_s",
            F.unix_timestamp("asked_at") - F.unix_timestamp("trip_updates_at"),
        )
        .fillna(False, ["cancelled", "assigned", "tracked"])
    )


# --- predictions ----------------------------------------------------------------------------------


def _epoch(ts: datetime) -> int:
    return int(ts.replace(tzinfo=UTC).timestamp())


def predict(
    spark: SparkSession, src: Sources, day: date, gold: str = GOLD, dim: Dim = DIM
) -> list[dict[str, Any]]:
    hist, base_rate = histories(spark, day, gold)
    rows = statuses(spark, src, moments(test_trips(spark, day, gold)), dim).collect()
    computed = datetime.now(UTC).replace(tzinfo=None, microsecond=0)
    out = []
    for r in rows:
        agency = AGENCY.get(r["agency_id"], r["agency_id"])
        route = r["route_key"].split("|")[1]
        route_slug = slug(agency, route)
        start = _epoch(r["scheduled_start"])
        asked = _epoch(r["asked_at"])
        trip = Trip("", "", "", f"{day:%Y%m%d}", "", start, agency, route, "", route_slug)
        age = r["snapshot_age_s"]
        status, fresh = live_status(
            float(age) if age is not None else None, r["cancelled"], r["tracked"], r["assigned"]
        )
        p, _ = probability(
            hist.get(route_slug), Departure("", start, 0, trip), status, asked, fresh
        )
        out.append(
            {
                "service_date": day,
                "trip_instance_key": r["trip_instance_key"],
                "lead_min": int(r["lead_min"]),
                "predicted_at": r["asked_at"],
                "route_slug": route_slug,
                "status": status,
                "probability": p,
                "ran": int(r["outcome"] == "RAN"),
                "outcome": r["outcome"],
                "base_rate": base_rate,
                "model": MODEL,
                "computed_at": computed,
            }
        )
    return out


def write(
    spark: SparkSession, day: date, rows: list[dict[str, Any]], table: str = PREDICTIONS
) -> None:
    """Replace the day's predictions: delete, then insert. Rerunning a day gives the same rows."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {table.rsplit('.', 1)[0]}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({SCHEMA}) USING iceberg")
    spark.sql(f"DELETE FROM {table} WHERE service_date = DATE '{day.isoformat()}'")
    if rows:
        spark.createDataFrame(rows, SCHEMA).writeTo(table).append()


def scored(spark: SparkSession, end: date, table: str = PREDICTIONS) -> list[Prediction]:
    """The last 28 test days' predictions, ready to score. Rows without a base rate (no history
    at all yet) are skipped: there is nothing to compare against."""
    start = end - timedelta(days=WINDOW_DAYS - 1)
    rows = (
        spark.table(table)
        .where(F.col("service_date").between(F.lit(start), F.lit(end)))
        .where(F.col("base_rate").isNotNull())
        .select("service_date", "lead_min", "status", "probability", "ran", "base_rate")
        .collect()
    )
    return [
        Prediction(
            r["service_date"],
            int(r["lead_min"]),
            r["status"],
            r["probability"],
            int(r["ran"]),
            float(r["base_rate"]),
        )
        for r in rows
    ]


def run(
    spark: SparkSession,
    days: list[date],
    target: Archive | None,
    source: Any,
    gold: str = GOLD,
    table: str = PREDICTIONS,
    dim: Dim = DIM,
) -> dict[str, Any]:
    """Predict each day (``source(day)`` gives its live data), store, then score and publish."""
    for day in days:
        rows = predict(spark, source(day), day, gold, dim)
        write(spark, day, rows, table)
        statuses_seen: dict[str, int] = defaultdict(int)
        for r in rows:
            statuses_seen[r["status"]] += 1
        log.info("%s: %d predictions %s", day, len(rows), dict(statuses_seen))
    preds = scored(spark, days[-1], table)
    doc = calibration_doc(preds, MODEL, datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"))
    if target is not None:
        publish(target, {"calibration.json": doc})
    o = doc["overall"]
    print(
        f"calibration over {len(doc['test_days'])} day(s): {o['scored']} scored predictions, "
        f"Brier {o['brier']} vs always-comes {o['brier_always_comes']} "
        f"and base rate {o['brier_base_rate']}"
    )
    return doc


def main(argv: list[str] | None = None) -> int:
    yesterday = datetime.now(LOCAL_TZ).date() - timedelta(days=1)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from", dest="first", type=date.fromisoformat, default=yesterday)
    parser.add_argument("--to", dest="last", type=date.fromisoformat, default=yesterday)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    spark = build_spark("calibration", LakeSettings.from_env(dict(os.environ)))
    spark.sparkContext.setLogLevel("ERROR")
    archive_uri, endpoint = os.environ["ARCHIVE_URI"], os.environ.get("ARCHIVE_S3_ENDPOINT")

    def source(day: date) -> Sources:
        return sources_from_archive(spark, archive_uri, endpoint, *data_window(day, day))

    days = [args.first + timedelta(days=i) for i in range((args.last - args.first).days + 1)]
    target = archive_from_uri(os.environ["EXPORT_URI"], endpoint)
    run(spark, days, target, source)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
