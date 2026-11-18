"""timetable_load: every archived static GTFS version into Iceberg as history, then trip instances.

For each archived version not yet loaded, oldest first:

1. ``dim.route`` and ``dim.stop``: SCD type 2 by natural key, applied with one ``MERGE`` each.
   Closed rows are never touched again, so a trip from last month still resolves to last
   month's route name and stops.
2. ``dim.route_version``, ``dim.trip``, ``dim.stop_time``, ``dim.service_date``: that version's
   own tables, partitioned by ``feed_version`` and written with partition overwrite (a rerun
   replaces, never duplicates). These carry NTA's raw ids, joined only within one version.
3. ``dim.feed_version``: rewritten last. It is the commit marker: a version is "loaded" only once
   it appears here, so a crash anywhere above is repaired by simply running the job again.

Then ``dim.trip_instance`` is (re)built for a range of service days: every scheduled trip on
each day, from the version that defines that day, with UTC start and end and the natural key.

Usage (inside the local stack)::

    docker compose -f local/docker-compose.yml run --rm spark \\
        python -m ghostbus_jobs.timetable_load --from 2026-11-14 --to 2026-11-22
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from py4j.protocol import Py4JJavaError
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ghostbus_core.lake import LakeSettings, table_properties
from ghostbus_core.timetable import (
    ACCEPTED_AGENCY_TIMEZONES,
    LOCAL_TZ,
    FeedVersion,
    route_key,
    row_hash,
    schedule_version_for,
    service_dates,
    service_day_origin,
)
from ghostbus_ingest.archive import Archive, archive_from_env, archive_from_uri
from ghostbus_ingest.static_archive import ArchivedVersion, extract, list_versions
from ghostbus_jobs.spark import build_spark

log = logging.getLogger("ghostbus.timetable_load")


@dataclass(frozen=True)
class Dim:
    """The timetable tables, all in one namespace. Tests pass a throwaway namespace."""

    ns: str = "lake.dim"

    def __getattr__(self, name: str) -> str:  # dim.trip -> "lake.dim.trip"
        if name in _TABLES:
            return f"{self.ns}.{name}"
        raise AttributeError(name)

    def all(self) -> list[str]:
        return [f"{self.ns}.{t}" for t in _TABLES]


_TABLES = (
    "feed_version",
    "route",
    "stop",
    "route_version",
    "trip",
    "stop_time",
    "service_date",
    "trip_instance",
)
DIM = Dim()

ROUTE_ATTRS = (
    "agency_id",
    "agency_name",
    "route_short_name",
    "route_long_name",
    "route_type",
    "route_color",
    "route_text_color",
)
STOP_ATTRS = (
    "stop_code",
    "stop_name",
    "stop_lat",
    "stop_lon",
    "location_type",
    "parent_station",
    "wheelchair_boarding",
)

_SCD2_COLS = """
    row_hash STRING NOT NULL,
    feed_version STRING NOT NULL,
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN NOT NULL
"""

DDL = {
    "feed_version": """
        feed_version STRING NOT NULL, sha256 STRING NOT NULL,
        valid_from TIMESTAMP NOT NULL, valid_to TIMESTAMP, is_current BOOLEAN NOT NULL,
        feed_start_date DATE, feed_end_date DATE, loaded_at TIMESTAMP NOT NULL,
        n_routes BIGINT, n_stops BIGINT, n_trips BIGINT, n_stop_times BIGINT
    """,
    "route": "route_key STRING NOT NULL, "
    + ", ".join(f"{c} STRING" for c in ROUTE_ATTRS)
    + ","
    + _SCD2_COLS,
    "stop": "stop_id STRING NOT NULL, "
    + ", ".join(f"{c} STRING" for c in STOP_ATTRS)
    + ","
    + _SCD2_COLS,
    "route_version": """
        feed_version STRING NOT NULL, route_id STRING NOT NULL, route_key STRING NOT NULL
    """,
    "trip": """
        feed_version STRING NOT NULL, trip_id STRING NOT NULL, route_id STRING,
        route_key STRING, service_id STRING, direction_id STRING, trip_headsign STRING,
        block_id STRING, shape_id STRING, first_departure_s INT, last_arrival_s INT,
        first_stop_id STRING, last_stop_id STRING, n_stops INT
    """,
    "stop_time": """
        feed_version STRING NOT NULL, trip_id STRING NOT NULL, stop_sequence INT NOT NULL,
        stop_id STRING, arrival_s INT, departure_s INT, pickup_type STRING,
        drop_off_type STRING, timepoint STRING
    """,
    "service_date": """
        feed_version STRING NOT NULL, service_id STRING NOT NULL, service_date DATE NOT NULL
    """,
    "trip_instance": """
        service_date DATE NOT NULL, trip_instance_key STRING NOT NULL,
        feed_version STRING NOT NULL, trip_id STRING NOT NULL, route_key STRING,
        direction_id STRING, first_departure_s INT, first_stop_id STRING,
        last_stop_id STRING, n_stops INT, duplicate_index INT NOT NULL,
        scheduled_start TIMESTAMP NOT NULL, scheduled_end TIMESTAMP NOT NULL
    """,
}
PARTITION = {
    "route_version": "feed_version",
    "trip": "feed_version",
    "stop_time": "feed_version",
    "service_date": "feed_version",
    "trip_instance": "service_date",
}


# --- setup ----------------------------------------------------------------------------------------


def ensure_tables(spark: SparkSession, dim: Dim = DIM) -> None:
    props = ", ".join(f"'{k}'='{v}'" for k, v in table_properties().items())
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {dim.ns}")
    for name, cols in DDL.items():
        part = f"PARTITIONED BY ({PARTITION[name]})" if name in PARTITION else ""
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {dim.ns}.{name} ({cols}) USING iceberg {part} "
            f"TBLPROPERTIES ({props})"
        )


def gtfs_seconds(col: Column) -> Column:
    """Spark twin of ``timetable.parse_gtfs_time``: 'HH:MM:SS' (HH may be > 23) to seconds."""
    parts = F.split(F.trim(col), ":")
    seconds = parts[0].cast("int") * 3600 + parts[1].cast("int") * 60 + parts[2].cast("int")
    return F.when(F.trim(col) == "", F.lit(None)).otherwise(seconds).cast("int")


def gtfs_time_text(col: Column) -> Column:
    """Spark twin of ``timetable.format_gtfs_time``."""
    return F.format_string(
        "%02d:%02d:%02d", F.floor(col / 3600), F.floor(col % 3600 / 60), col % 60
    )


def read_csv(spark: SparkSession, folder: Path | str, name: str) -> DataFrame:
    path = f"{str(folder).rstrip('/')}/{name}"
    df = spark.read.option("header", True).option("encoding", "UTF-8").csv(path)
    return df.toDF(*[c.lstrip("﻿").strip() for c in df.columns])


def _rows(folder: Path, name: str) -> list[dict[str, str]]:
    import csv

    path = folder / name
    if not path.exists():
        return []
    with path.open(encoding="utf-8-sig", newline="") as f:
        return [dict(r) for r in csv.DictReader(f)]


# --- SCD2 dimensions (small: built in the driver with the same functions as the reference) --------


def route_snapshot(folder: Path) -> tuple[dict[str, dict[str, str]], dict[str, str], int]:
    """(route_key -> attributes, route_id -> route_key, natural-key collisions)."""
    agencies = {a["agency_id"]: a for a in _rows(folder, "agency.txt")}
    for a in agencies.values():
        if a.get("agency_timezone", "") not in ACCEPTED_AGENCY_TIMEZONES:
            raise ValueError(f"unexpected agency_timezone {a.get('agency_timezone')!r}")
    attrs: dict[str, dict[str, str]] = {}
    id_to_key: dict[str, str] = {}
    collisions = 0
    for r in sorted(_rows(folder, "routes.txt"), key=lambda r: r["route_id"]):
        agency_id = r.get("agency_id", "") or next(iter(agencies), "")
        key = route_key(
            agency_id, r.get("route_short_name", ""), r["route_type"], r.get("route_long_name", "")
        )
        id_to_key[r["route_id"]] = key
        row = {
            "agency_id": agency_id,
            "agency_name": agencies.get(agency_id, {}).get("agency_name", ""),
            "route_short_name": r.get("route_short_name", ""),
            "route_long_name": r.get("route_long_name", ""),
            "route_type": r["route_type"],
            "route_color": r.get("route_color", ""),
            "route_text_color": r.get("route_text_color", ""),
        }
        if key in attrs:
            collisions += 1  # first route_id (sorted) wins; all ids still map to the key
            continue
        attrs[key] = row
    return attrs, id_to_key, collisions


def stop_snapshot(folder: Path) -> dict[str, dict[str, str]]:
    return {s["stop_id"]: {a: s.get(a, "") for a in STOP_ATTRS} for s in _rows(folder, "stops.txt")}


def scd2_merge(
    spark: SparkSession,
    table: str,
    key_col: str,
    attr_cols: Sequence[str],
    snapshot: Mapping[str, Mapping[str, str]],
    version: FeedVersion,
) -> None:
    """Apply one version's snapshot to an SCD2 table in a single atomic MERGE.

    Source rows: every snapshot row keyed by its natural key (closes a changed current row or
    inserts a new key), plus a second copy with a NULL key for changed rows (inserts the new
    definition). Current rows missing from the snapshot are closed by NOT MATCHED BY SOURCE.
    Re-running a version matches every row with an equal hash and changes nothing.
    """
    schema = StructType(
        [StructField(key_col, StringType(), False)]
        + [StructField(c, StringType(), True) for c in attr_cols]
        + [StructField("row_hash", StringType(), False)]
    )
    rows = [
        (k, *(a.get(c, "") for c in attr_cols), row_hash({c: a.get(c, "") for c in attr_cols}))
        for k, a in sorted(snapshot.items())
    ]
    spark.createDataFrame(rows, schema).createOrReplaceTempView("scd2_snapshot")
    vf = version.valid_from.strftime("%Y-%m-%d %H:%M:%S")
    attr_list = ", ".join(attr_cols)
    src_attrs = ", ".join(f"s.{c}" for c in attr_cols)
    spark.sql(f"""
        MERGE INTO {table} t
        USING (
            SELECT {key_col} AS merge_key, * FROM scd2_snapshot
            UNION ALL
            SELECT NULL AS merge_key, s.*
            FROM scd2_snapshot s
            JOIN {table} c ON c.{key_col} = s.{key_col} AND c.is_current
            WHERE c.row_hash <> s.row_hash
        ) src
        ON t.{key_col} = src.merge_key AND t.is_current
        WHEN MATCHED AND t.row_hash <> src.row_hash THEN
            UPDATE SET valid_to = TIMESTAMP '{vf}', is_current = false
        WHEN NOT MATCHED THEN
            INSERT ({key_col}, {attr_list}, row_hash, feed_version, valid_from, valid_to,
                    is_current)
            VALUES (src.{key_col}, {src_attrs.replace("s.", "src.")}, src.row_hash,
                    '{version.feed_version}', TIMESTAMP '{vf}', NULL, true)
        WHEN NOT MATCHED BY SOURCE AND t.is_current THEN
            UPDATE SET valid_to = TIMESTAMP '{vf}', is_current = false
    """)


# --- per-version tables (large: Spark) ------------------------------------------------------------


def load_version_tables(
    spark: SparkSession,
    folder: Path,
    version: FeedVersion,
    id_to_key: Mapping[str, str],
    dim: Dim = DIM,
    spark_folder: str | None = None,
) -> dict[str, int]:
    fv = F.lit(version.feed_version)

    route_map = spark.createDataFrame(
        sorted(id_to_key.items()), "route_id STRING, route_key STRING"
    ).withColumn("feed_version", fv)
    route_map.select("feed_version", "route_id", "route_key").writeTo(
        dim.route_version
    ).overwritePartitions()

    st = read_csv(spark, spark_folder or folder, "stop_times.txt").select(
        fv.alias("feed_version"),
        "trip_id",
        F.col("stop_sequence").cast("int").alias("stop_sequence"),
        "stop_id",
        gtfs_seconds(F.col("arrival_time")).alias("arrival_s"),
        gtfs_seconds(F.col("departure_time")).alias("departure_s"),
        "pickup_type",
        "drop_off_type",
        "timepoint",
    )
    st.writeTo(dim.stop_time).overwritePartitions()

    stored = spark.table(dim.stop_time).where(F.col("feed_version") == version.feed_version)
    agg = stored.groupBy("trip_id").agg(
        F.min_by(F.coalesce("departure_s", "arrival_s"), "stop_sequence").alias(
            "first_departure_s"
        ),
        F.min_by("stop_id", "stop_sequence").alias("first_stop_id"),
        F.max_by(F.coalesce("arrival_s", "departure_s"), "stop_sequence").alias("last_arrival_s"),
        F.max_by("stop_id", "stop_sequence").alias("last_stop_id"),
        F.count("*").cast("int").alias("n_stops"),
    )
    trips = (
        read_csv(spark, spark_folder or folder, "trips.txt")
        .join(route_map.select("route_id", "route_key"), "route_id", "left")
        .join(agg, "trip_id", "left")
        .select(
            fv.alias("feed_version"),
            "trip_id",
            "route_id",
            "route_key",
            "service_id",
            "direction_id",
            "trip_headsign",
            "block_id",
            "shape_id",
            "first_departure_s",
            "last_arrival_s",
            "first_stop_id",
            "last_stop_id",
            "n_stops",
        )
    )
    trips.writeTo(dim.trip).overwritePartitions()

    days = service_dates(_rows(folder, "calendar.txt"), _rows(folder, "calendar_dates.txt"))
    sd_rows = [
        (version.feed_version, sid, d) for sid, ds in sorted(days.items()) for d in sorted(ds)
    ]
    sd_schema = "feed_version STRING, service_id STRING, service_date DATE"
    spark.createDataFrame(sd_rows, sd_schema).writeTo(dim.service_date).overwritePartitions()

    counts = (
        spark.table(dim.trip)
        .where(F.col("feed_version") == version.feed_version)
        .agg(
            F.count("*").alias("n_trips"),
            F.sum("n_stops").alias("n_stop_times"),
            F.sum(F.col("route_key").isNull().cast("int")).alias("trips_without_route"),
            F.sum(F.col("n_stops").isNull().cast("int")).alias("trips_without_stops"),
        )
        .first()
    )
    assert counts is not None
    return {k: int(counts[k] or 0) for k in counts.asDict()}


# --- feed_version (the commit marker) -------------------------------------------------------------


def loaded_versions(spark: SparkSession, dim: Dim = DIM) -> list[FeedVersion]:
    rows = spark.table(dim.feed_version).collect()
    return [
        FeedVersion(
            r["feed_version"],
            r["sha256"],
            r["valid_from"].replace(tzinfo=UTC),
            r["feed_start_date"],
            r["feed_end_date"],
        )
        for r in rows
    ]


def write_feed_versions(
    spark: SparkSession,
    versions: Sequence[FeedVersion],
    stats: Mapping[str, Mapping[str, int]],
    dim: Dim = DIM,
) -> None:
    """Rewrite the whole (tiny) table: valid_to of each version is the next one's valid_from."""
    existing = {r["sha256"]: r for r in spark.table(dim.feed_version).collect()}
    now = datetime.now(UTC).replace(tzinfo=None)
    rows = []
    for i, v in enumerate(versions):
        nxt = versions[i + 1].valid_from if i + 1 < len(versions) else None
        s = stats.get(v.sha256)
        old = existing.get(v.sha256)
        rows.append(
            (
                v.feed_version,
                v.sha256,
                v.valid_from.replace(tzinfo=None),
                nxt.replace(tzinfo=None) if nxt else None,
                nxt is None,
                v.feed_start_date,
                v.feed_end_date,
                old["loaded_at"] if old and not s else now,
                *(
                    (s or {}).get(c, old[c] if old else None)
                    for c in ("n_routes", "n_stops", "n_trips", "n_stop_times")
                ),
            )
        )
    schema = spark.table(dim.feed_version).schema
    spark.createDataFrame(rows, schema).writeTo(dim.feed_version).overwrite(F.lit(True))


def stage_for_spark(folder: Path, sha256: str) -> str:
    """Where Spark reads one version's CSVs from.

    In local mode the driver's extracted folder is fine. On a cluster (Glue) the executors run on
    other machines and cannot see the driver's disk, so ``STATIC_STAGE_URI`` names shared storage
    (``s3://...``) and the files are copied there first. Found on the first Glue run.
    """
    stage = os.environ.get("STATIC_STAGE_URI", "").rstrip("/")
    if not stage:
        return str(folder)
    target = archive_from_uri(stage + "/")
    for f in sorted(folder.glob("*.txt")):
        target.put_file(f"{sha256}/{f.name}", f, "text/csv")
    return f"{stage}/{sha256}"


def load_versions(
    spark: SparkSession, archive: Archive, workdir: Path, rebuild: bool = False, dim: Dim = DIM
) -> list[FeedVersion]:
    archived = list_versions(archive)
    if not archived:
        raise SystemExit("no static versions in the archive yet (is the static watcher running?)")
    ensure_tables(spark, dim)
    if rebuild:
        # Empty rather than drop: a whole-table DELETE is a metadata-only commit in Iceberg and
        # works on every catalog. S3 Tables refused DROP here ("only supports dropping tables with
        # purge enabled") although the statement said PURGE.
        for t in dim.all():
            spark.sql(f"DELETE FROM {t}")
    done = {v.sha256 for v in loaded_versions(spark, dim)}
    latest_loaded = max((v.valid_from for v in loaded_versions(spark, dim)), default=None)
    todo: list[ArchivedVersion] = [a for a in archived if a.version.sha256 not in done]
    for a in todo:
        if latest_loaded and a.version.valid_from < latest_loaded:
            raise SystemExit(
                f"version {a.version.feed_version} predates loaded history; run with --rebuild"
            )

    stats: dict[str, dict[str, int]] = {}
    loaded = [a.version for a in archived if a.version.sha256 in done]
    for a in todo:
        v = a.version
        log.info("loading %s (published %s)", v.feed_version, v.valid_from.isoformat())
        folder = extract(archive, a, workdir)
        routes, id_to_key, collisions = route_snapshot(folder)
        stops = stop_snapshot(folder)
        scd2_merge(spark, dim.route, "route_key", ROUTE_ATTRS, routes, v)
        scd2_merge(spark, dim.stop, "stop_id", STOP_ATTRS, stops, v)
        counts = load_version_tables(
            spark, folder, v, id_to_key, dim, spark_folder=stage_for_spark(folder, v.sha256)
        )
        counts |= {"n_routes": len(routes), "n_stops": len(stops), "route_collisions": collisions}
        log.info("loaded %s: %s", v.feed_version, counts)
        stats[v.sha256] = counts
        loaded.append(v)
        write_feed_versions(spark, loaded, stats, dim)  # commit marker, after everything else
    return loaded


# --- trip instances -------------------------------------------------------------------------------


def day_plan(
    versions: Sequence[FeedVersion], first: date, last: date
) -> list[tuple[date, str, datetime]]:
    """(service date, version that defines it, UTC origin of its GTFS times) for each day."""
    out = []
    d = first
    while d <= last:
        v = schedule_version_for(versions, d)
        if v is not None:
            out.append((d, v.feed_version, service_day_origin(d).replace(tzinfo=None)))
        d += timedelta(days=1)
    return out


def build_trip_instances(
    spark: SparkSession, versions: Sequence[FeedVersion], first: date, last: date, dim: Dim = DIM
) -> int:
    plan = day_plan(versions, first, last)
    if not plan:
        return 0
    days = spark.createDataFrame(
        plan,
        StructType(
            [
                StructField("service_date", DateType(), False),
                StructField("feed_version", StringType(), False),
                StructField("origin", TimestampType(), False),
            ]
        ),
    )
    running = days.join(spark.table(dim.service_date), ["feed_version", "service_date"])
    trips = running.join(spark.table(dim.trip), ["feed_version", "service_id"])
    natural = ["service_date", "route_key", "direction_id", "first_departure_s", "first_stop_id"]
    tie_break = ["last_stop_id", "n_stops", "last_arrival_s", "trip_id"]
    dup = F.row_number().over(Window.partitionBy(*natural).orderBy(*tie_break)) - 1
    base_key = F.concat_ws(
        "|",
        "route_key",
        "direction_id",
        F.date_format("service_date", "yyyyMMdd"),
        gtfs_time_text(F.col("first_departure_s")),
        "first_stop_id",
    )
    out = (
        trips.withColumn("duplicate_index", dup.cast("int"))
        .withColumn(
            "trip_instance_key",
            F.when(F.col("duplicate_index") == 0, base_key).otherwise(
                F.concat_ws("|", base_key, F.col("duplicate_index").cast("string"))
            ),
        )
        .withColumn(
            "scheduled_start",
            F.timestamp_seconds(F.unix_timestamp("origin") + F.col("first_departure_s")),
        )
        .withColumn(
            "scheduled_end",
            F.timestamp_seconds(F.unix_timestamp("origin") + F.col("last_arrival_s")),
        )
        .select(
            "service_date",
            "trip_instance_key",
            "feed_version",
            "trip_id",
            "route_key",
            "direction_id",
            "first_departure_s",
            "first_stop_id",
            "last_stop_id",
            "n_stops",
            "duplicate_index",
            "scheduled_start",
            "scheduled_end",
        )
    )
    out.writeTo(dim.trip_instance).overwritePartitions()
    return int(
        spark.table(dim.trip_instance)
        .where(F.col("service_date").between(F.lit(first), F.lit(last)))
        .count()
    )


# --- entry point ----------------------------------------------------------------------------------


def run(
    spark: SparkSession,
    archive: Archive,
    workdir: Path,
    first: date,
    last: date,
    rebuild: bool = False,
    dim: Dim = DIM,
) -> tuple[list[FeedVersion], int]:
    versions = load_versions(spark, archive, workdir, rebuild=rebuild, dim=dim)
    n = build_trip_instances(spark, versions, first, last, dim)
    return versions, n


def _local_today() -> date:
    return datetime.now(LOCAL_TZ).date()


def main(argv: Iterable[str] | None = None) -> int:
    today = _local_today()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--from", dest="first", type=date.fromisoformat, default=today - timedelta(days=1)
    )
    parser.add_argument(
        "--to", dest="last", type=date.fromisoformat, default=today + timedelta(days=7)
    )
    parser.add_argument("--rebuild", action="store_true", help="drop the dim tables and reload all")
    parser.add_argument("--workdir", type=Path, default=Path("/tmp/ghostbus-static"))
    args = parser.parse_args(list(argv) if argv is not None else None)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    spark = build_spark(
        "timetable_load",
        LakeSettings.from_env(dict(os.environ)),
        {"spark.driver.memory": os.environ.get("SPARK_DRIVER_MEMORY", "4g")},
    )
    spark.sparkContext.setLogLevel("WARN")
    # Every step is idempotent and the feed_version row is written last, so a failed attempt
    # (seen locally: SeaweedFS occasionally truncates a read under parallel load) is repaired by
    # running again from the top.
    attempts = int(os.environ.get("TIMETABLE_LOAD_ATTEMPTS", "3"))
    for attempt in range(1, attempts + 1):
        try:
            versions, n = run(
                spark,
                archive_from_env(os.environ),
                args.workdir,
                args.first,
                args.last,
                args.rebuild,
            )
            break
        except Py4JJavaError as exc:
            if attempt == attempts:
                raise
            log.warning("attempt %d failed (%s); retrying", attempt, str(exc).splitlines()[0])
    print(f"versions loaded: {len(versions)}; trip instances {args.first}..{args.last}: {n:,}")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
