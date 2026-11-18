"""Nightly data-quality gates and the daily summary email (roadmap 1.1 and 1.2).

Runs after ``reconcile``. For one service day it gathers the numbers the checks need (from the
lake and from that day's raw archive), evaluates them with ``ghostbus_core.quality``, upserts the
result into ``gold.data_quality``, prints the summary and, if ``ALERTS_TOPIC_ARN`` is set, emails
it through SNS with OK / WARN / FAIL in the subject.

    docker compose -f local/docker-compose.yml run --rm spark \\
        python -m ghostbus_jobs.quality --date 2026-11-16
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import UTC, date, datetime, time, timedelta

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from ghostbus_core.lake import LakeSettings
from ghostbus_core.outcomes import DEFAULT
from ghostbus_core.quality import Check, DayStats, evaluate, overall, render
from ghostbus_core.timetable import LOCAL_TZ
from ghostbus_jobs.outcome_report import AGENCY
from ghostbus_jobs.outcomes import GOLD, Sources, sources_from_archive, with_trip_version
from ghostbus_jobs.spark import build_spark
from ghostbus_jobs.timetable_load import DIM, Dim

log = logging.getLogger("ghostbus.quality")

QUALITY = "lake.gold.data_quality"
BATCH_JOBS = ("ghostbus-timetable-load", "ghostbus-reconcile", "ghostbus-quality")
FLEX_DPU_HOUR_USD = 0.29


def in_rates() -> Column:
    """Bus trips whose whole window was collected. A function, not a module constant: building a
    Column needs an active SparkContext, and Glue imports this module before Spark starts (the
    first cloud run failed exactly there)."""
    return F.col("route_type").eqNullSafe("3") & ~F.col("tu_gap") & ~F.col("vp_gap")


def day_bounds(day: date) -> tuple[datetime, datetime]:
    """The service day as local midnight to local midnight, in UTC (23 or 25 h on clock changes)."""
    start = datetime.combine(day, time(0), tzinfo=LOCAL_TZ)
    end = datetime.combine(day + timedelta(days=1), time(0), tzinfo=LOCAL_TZ)
    return start.astimezone(UTC), end.astimezone(UTC)


def _count(df: DataFrame) -> int:
    return int(df.count())


def gather(
    spark: SparkSession,
    day: date,
    src: Sources,
    gold: str = GOLD,
    dim: Dim = DIM,
    batch_cost_usd: float | None = None,
) -> DayStats:
    inst = spark.table(dim.trip_instance)
    scheduled = _count(inst.where(F.col("service_date") == F.lit(day)))
    week_away = None
    for other in (day + timedelta(days=7), day - timedelta(days=7)):
        n = _count(inst.where(F.col("service_date") == F.lit(other)))
        if n:
            week_away = n
            break

    g = spark.table(gold).where(F.col("service_date") == F.lit(day))
    agg = g.agg(
        F.count("*").alias("decided"),
        F.sum(F.col("route_type").eqNullSafe("3").cast("int")).alias("bus"),
        F.sum(
            (F.col("route_type").eqNullSafe("3") & (F.col("outcome") == "NO_DATA")).cast("int")
        ).alias("no_data"),
        *[
            F.sum((in_rates() & F.col("outcome").isin(*outs)).cast("int")).alias(name)
            for name, outs in {
                "in_rates": [
                    "RAN",
                    "GHOST",
                    "PHANTOM_LIVE",
                    "VANISHED",
                    "CANCELLED_EARLY",
                    "CANCELLED_LATE",
                ],
                "ran": ["RAN"],
                "untracked": ["GHOST", "PHANTOM_LIVE"],
                "vanished": ["VANISHED"],
                "cancelled_early": ["CANCELLED_EARLY"],
                "cancelled_late": ["CANCELLED_LATE"],
            }.items()
        ],
    ).first()
    assert agg is not None
    a = {k: int(agg[k] or 0) for k in agg.asDict()}

    trailing = (
        spark.table(gold)
        .where(
            F.col("service_date").between(
                F.lit(day - timedelta(days=7)), F.lit(day - timedelta(days=1))
            )
        )
        .where(in_rates())
        .agg(
            F.count("*").alias("n"),
            F.sum(F.col("outcome").isin("GHOST", "PHANTOM_LIVE").cast("int")).alias("u"),
        )
        .first()
    )
    assert trailing is not None

    worst = (
        g.where(in_rates())
        .groupBy("agency_id", F.split("route_key", r"\|")[1].alias("route"))
        .agg(
            F.count("*").alias("n"),
            F.sum(F.col("outcome").isin("GHOST", "PHANTOM_LIVE").cast("int")).alias("u"),
        )
        .where("u > 0")
        .orderBy(F.desc("u"), F.desc("n"))
        .limit(5)
        .collect()
    )

    start, end = day_bounds(day)
    lo, hi = F.lit(start.replace(tzinfo=None)), F.lit(end.replace(tzinfo=None))
    minutes = _count(
        src.heartbeats.where(F.col("collected") & F.col("fetched_at").between(lo, hi))
        .select(F.date_trunc("minute", "fetched_at"))
        .distinct()
    )
    rt = with_trip_version(
        src.trip_updates.where(
            F.col("fetched_at").between(lo, hi) & F.col("trip_id").isNotNull()
        ).select("fetched_at", "trip_id"),
        spark,
        dim,
    ).agg(F.count("*").alias("n"), F.sum((~F.col("version_found")).cast("int")).alias("un"))
    rt_row = rt.first()
    assert rt_row is not None

    return DayStats(
        service_date=day,
        scheduled=scheduled,
        scheduled_week_away=week_away,
        decided=a["decided"],
        bus_trips=a["bus"],
        no_data=a["no_data"],
        in_rates=a["in_rates"],
        ran=a["ran"],
        untracked=a["untracked"],
        vanished=a["vanished"],
        cancelled_early=a["cancelled_early"],
        cancelled_late=a["cancelled_late"],
        minutes_expected=round((end - start).total_seconds() / 60),
        minutes_collected=minutes,
        rt_with_id=int(rt_row["n"] or 0),
        rt_unmatched=int(rt_row["un"] or 0),
        trailing_in_rates=int(trailing["n"] or 0),
        trailing_untracked=int(trailing["u"] or 0),
        worst_routes=[
            (f"{AGENCY.get(r['agency_id'], r['agency_id'])} {r['route']}", int(r["n"]), int(r["u"]))
            for r in worst
        ],
        batch_cost_usd=batch_cost_usd,
    )


def write(spark: SparkSession, day: date, checks: list[Check], table: str = QUALITY) -> None:
    """Upsert one row per (service_date, check). Deterministic input, so MERGE accepts it."""
    status = overall(checks)
    now = datetime.now(UTC).replace(tzinfo=None, microsecond=0)
    rows = [
        (day, c.name, c.value, c.status, c.detail, status, DEFAULT.version, now) for c in checks
    ]
    schema = (
        "service_date DATE, check_name STRING, value DOUBLE, status STRING, detail STRING, "
        "overall STRING, params_version STRING, computed_at TIMESTAMP"
    )
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {table.rsplit('.', 1)[0]}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({schema}) USING iceberg")
    spark.createDataFrame(rows, schema).createOrReplaceTempView("new_quality")
    spark.sql(f"""
        MERGE INTO {table} t USING new_quality s
        ON t.service_date = s.service_date AND t.check_name = s.check_name
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def batch_cost(region: str, since: datetime) -> float | None:
    """What the Glue jobs that finished since ``since`` cost (Flex price). None outside AWS."""
    try:
        import boto3

        glue = boto3.client("glue", region_name=region)
        dpu_s = 0.0
        for job in BATCH_JOBS:
            for run in glue.get_job_runs(JobName=job, MaxResults=10)["JobRuns"]:
                if run.get("StartedOn") and run["StartedOn"] >= since and run.get("DPUSeconds"):
                    dpu_s += float(run["DPUSeconds"])
        return dpu_s / 3600 * FLEX_DPU_HOUR_USD
    except Exception as exc:  # no credentials or no Glue: the cost line is optional
        log.info("batch cost unavailable: %s", exc)
        return None


def publish(topic_arn: str, region: str, subject: str, body: str) -> None:
    import boto3

    boto3.client("sns", region_name=region).publish(
        TopicArn=topic_arn, Subject=subject[:100], Message=body
    )


def main(argv: list[str] | None = None) -> int:
    yesterday = datetime.now(LOCAL_TZ).date() - timedelta(days=1)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, default=yesterday)
    parser.add_argument("--no-email", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    region = os.environ.get("AWS_REGION", "eu-north-1")
    spark = build_spark("quality", LakeSettings.from_env(dict(os.environ)))
    spark.sparkContext.setLogLevel("ERROR")
    start, end = day_bounds(args.date)
    src = sources_from_archive(
        spark, os.environ["ARCHIVE_URI"], os.environ.get("ARCHIVE_S3_ENDPOINT"), start, end
    )
    cost = (
        batch_cost(region, datetime.now(UTC) - timedelta(hours=12))
        if os.environ.get("ALERTS_TOPIC_ARN")
        else None
    )
    stats = gather(spark, args.date, src, batch_cost_usd=cost)
    checks = evaluate(stats)
    write(spark, args.date, checks)
    subject, body = render(stats, checks)
    print(subject + "\n\n" + body)
    topic = os.environ.get("ALERTS_TOPIC_ARN", "")
    if topic and not args.no_email:
        publish(topic, region, subject, body)
        log.info("summary emailed via %s", topic)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
