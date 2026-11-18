"""Sprint 2 measure: how well archived realtime trips join to the versioned timetable.

For every TripUpdates and Vehicles snapshot in the archive (or a sample), each trip is matched
against the static version that was live when the snapshot was fetched:

* ``trip_id`` found in that version's ``dim.trip`` (the headline unmatched rate);
* realtime ``start_time`` equal to the scheduled first departure (are we joining the right trip?);
* resolved to a scheduled trip instance (natural key) on its ``start_date``;
* ADDED trips (no ``trip_id``): ``route_id`` resolvable to a durable route key.

Also prints a summary of the loaded timetable tables. Output is Markdown for
``docs/measurements/02-timetable-match.md``.

    docker compose -f local/docker-compose.yml run --rm spark \
        python -m ghostbus_jobs.rt_match_report
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Iterator
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ghostbus_core.archive_layout import parse_snapshot_key
from ghostbus_core.feeds import Feed
from ghostbus_core.gtfsr import parse_feed
from ghostbus_core.lake import LakeSettings
from ghostbus_core.timetable import FeedVersion, version_live_at
from ghostbus_ingest.archive import Archive, archive_from_env, decompress
from ghostbus_jobs.spark import build_spark
from ghostbus_jobs.timetable_load import DIM, gtfs_time_text, loaded_versions

SCHEMA = (
    "feed STRING, fetched_at TIMESTAMP, feed_version STRING, relationship STRING, "
    "trip_id STRING, route_id STRING, direction_id STRING, start_time STRING, start_date STRING"
)


def table(headers: list[str], rows: list[list[object]]) -> str:
    out = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def pct(a: int, b: int) -> str:
    return f"{100 * a / b:.2f}%" if b else "-"


def snapshot_keys(archive: Archive, feed: Feed, sample: int) -> list[str]:
    keys = sorted(archive.list_keys(f"raw/feed={feed.value}/"))
    if sample and len(keys) > sample:
        step = len(keys) / sample
        keys = [keys[int(i * step)] for i in range(sample)]
    return keys


def rows(
    archive: Archive, feed: Feed, keys: list[str], versions: list[FeedVersion]
) -> Iterator[tuple[object, ...]]:
    for key in keys:
        packed = archive.get_bytes(key)
        if packed is None:
            continue
        _, ms = parse_snapshot_key(key)
        at = datetime.fromtimestamp(ms / 1000, tz=UTC)
        live = version_live_at(versions, at)
        msg = parse_feed(decompress(packed))
        for e in msg.entity:
            trip = e.trip_update.trip if feed is Feed.TRIP_UPDATES else e.vehicle.trip
            rel = trip.DESCRIPTOR.fields_by_name["schedule_relationship"].enum_type
            assert rel is not None
            yield (
                feed.value,
                at.replace(tzinfo=None),
                live.feed_version if live else None,
                rel.values_by_number[trip.schedule_relationship].name,
                trip.trip_id if trip.HasField("trip_id") else None,
                trip.route_id if trip.HasField("route_id") else None,
                str(trip.direction_id) if trip.HasField("direction_id") else None,
                trip.start_time if trip.HasField("start_time") else None,
                trip.start_date if trip.HasField("start_date") else None,
            )


def match(spark: SparkSession, rt: DataFrame) -> DataFrame:
    trips = spark.table(DIM.trip).select(
        "feed_version",
        "trip_id",
        F.col("route_key").alias("trip_route_key"),
        gtfs_time_text(F.col("first_departure_s")).alias("scheduled_start_time"),
    )
    instances = spark.table(DIM.trip_instance).select(
        F.col("service_date").alias("i_date"),
        F.col("feed_version").alias("i_version"),
        F.col("trip_id").alias("i_trip_id"),
        "trip_instance_key",
    )
    routes = spark.table(DIM.route_version).withColumnRenamed("route_key", "rt_route_key")
    return (
        rt.withColumn("service_date", F.to_date("start_date", "yyyyMMdd"))
        .join(trips, ["feed_version", "trip_id"], "left")
        .join(
            instances,
            (F.col("feed_version") == F.col("i_version"))
            & (F.col("trip_id") == F.col("i_trip_id"))
            & (F.col("service_date") == F.col("i_date")),
            "left",
        )
        .drop("i_date", "i_version", "i_trip_id")
        .join(routes, ["feed_version", "route_id"], "left")
        .withColumn("agency", F.split(F.coalesce("trip_route_key", "rt_route_key"), r"\|")[0])
    )


def feed_section(m: DataFrame, feed: Feed) -> str:
    f = m.where(F.col("feed") == feed.value)
    total = f.count()
    snaps = f.select("fetched_at").distinct().count()
    with_id = f.where(F.col("trip_id").isNotNull() & (F.col("relationship") != "ADDED"))
    n_id = with_id.count()
    matched = with_id.where(
        F.col("trip_route_key").isNotNull() | F.col("scheduled_start_time").isNotNull()
    )
    n_matched = matched.count()
    n_start_ok = matched.where(F.col("start_time") == F.col("scheduled_start_time")).count()
    n_instance = matched.where(F.col("trip_instance_key").isNotNull()).count()
    added = f.where(F.col("relationship") == "ADDED")
    n_added = added.count()
    n_added_route = added.where(F.col("rt_route_key").isNotNull()).count()
    lines = [
        f"{snaps} snapshots, {total:,} trips.",
        "",
        table(
            ["Check", "Trips", "Rate"],
            [
                ["Has a `trip_id` (not ADDED)", f"{n_id:,}", pct(n_id, total)],
                ["`trip_id` found in the live version", f"{n_matched:,}", pct(n_matched, n_id)],
                ["**Unmatched `trip_id`**", f"{n_id - n_matched:,}", pct(n_id - n_matched, n_id)],
                [
                    "`start_time` equals scheduled first departure",
                    f"{n_start_ok:,}",
                    pct(n_start_ok, n_matched),
                ],
                [
                    "Resolved to a trip instance on `start_date`",
                    f"{n_instance:,}",
                    pct(n_instance, n_matched),
                ],
                ["ADDED trips", f"{n_added:,}", pct(n_added, total)],
                [
                    "ADDED with a resolvable `route_id`",
                    f"{n_added_route:,}",
                    pct(n_added_route, n_added),
                ],
            ],
        ),
    ]
    by_agency = (
        with_id.groupBy("agency")
        .agg(
            F.count("*").alias("n"),
            F.sum((F.col("trip_route_key").isNull()).cast("int")).alias("unmatched"),
            F.sum(F.col("trip_instance_key").isNull().cast("int")).alias("no_instance"),
        )
        .orderBy(F.desc("n"))
        .collect()
    )
    lines += [
        "",
        table(
            ["Agency id", "Trips with id", "Unmatched", "No trip instance"],
            [
                [
                    r["agency"],
                    f"{r['n']:,}",
                    pct(r["unmatched"], r["n"]),
                    pct(r["no_instance"], r["n"]),
                ]
                for r in by_agency
            ],
        ),
    ]
    odd = (
        matched.where(F.col("start_time") != F.col("scheduled_start_time"))
        .select("trip_id", "start_time", "scheduled_start_time", "agency")
        .distinct()
        .limit(5)
        .collect()
    )
    if odd:
        lines += ["", "Examples where `start_time` differs from the schedule:", ""]
        lines += [
            f"- `{r['trip_id']}` ({r['agency']}): realtime {r['start_time']}, "
            f"scheduled {r['scheduled_start_time']}"
            for r in odd
        ]
    return "\n".join(lines)


def timetable_section(spark: SparkSession, versions: list[FeedVersion]) -> str:
    routes = spark.table(DIM.route)
    stops = spark.table(DIM.stop)
    inst = spark.table(DIM.trip_instance)
    per_day = (
        inst.groupBy("service_date")
        .agg(
            F.count("*").alias("n"),
            F.sum((F.col("duplicate_index") > 0).cast("int")).alias("dups"),
            F.min("scheduled_start").alias("first"),
            F.max("scheduled_end").alias("last"),
        )
        .orderBy("service_date")
        .collect()
    )
    return "\n".join(
        [
            table(
                ["Feed version", "Valid from (UTC)", "Feed dates"],
                [
                    [
                        v.feed_version,
                        f"{v.valid_from:%Y-%m-%d %H:%M}",
                        f"{v.feed_start_date} to {v.feed_end_date}",
                    ]
                    for v in versions
                ],
            ),
            "",
            f"- `dim.route`: {routes.count():,} rows, "
            f"{routes.where('is_current').count():,} current",
            f"- `dim.stop`: {stops.count():,} rows, {stops.where('is_current').count():,} current",
            "",
            table(
                [
                    "Service date",
                    "Trip instances",
                    "With duplicate index",
                    "First start (UTC)",
                    "Last end (UTC)",
                ],
                [
                    [r["service_date"], f"{r['n']:,}", r["dups"], r["first"], r["last"]]
                    for r in per_day
                ],
            ),
        ]
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample", type=int, default=0, help="snapshots per feed (0 = all)")
    args = parser.parse_args(argv)
    spark = build_spark("rt_match_report", LakeSettings.from_env(dict(os.environ)))
    spark.sparkContext.setLogLevel("ERROR")
    archive = archive_from_env(os.environ)
    versions = sorted(loaded_versions(spark), key=lambda v: v.valid_from)
    if not versions:
        print("no timetable loaded; run ghostbus_jobs.timetable_load first")
        return 1
    data = [
        r
        for feed in (Feed.TRIP_UPDATES, Feed.VEHICLES)
        for r in rows(archive, feed, snapshot_keys(archive, feed, args.sample), versions)
    ]
    m = match(spark, spark.createDataFrame(data, SCHEMA)).cache()
    print("## Timetable loaded\n")
    print(timetable_section(spark, versions))
    for feed in (Feed.TRIP_UPDATES, Feed.VEHICLES):
        print(f"\n## {feed.value}: realtime against the live timetable version\n")
        print(feed_section(m, feed))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
