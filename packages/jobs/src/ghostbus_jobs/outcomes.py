"""Trip outcomes in Spark: evidence from silver-shaped data, classified by the shared rules.

Two sources produce identically shaped inputs, so everything downstream is one code path:

* :func:`sources_from_silver`: the streaming silver tables (live, provisional).
* :func:`sources_from_archive`: raw archived snapshots, normalised with the same
  ``ghostbus_core.gtfsr`` code and passed through the same ``stream.*_frame`` functions the
  streaming queries use (in batch, their dedupe drops duplicates and the watermark is a no-op).
  This is the reconcile and backfill path, and it needs neither Kafka nor the stream.

Outcomes go to ``gold.trip_outcomes`` by ``MERGE`` on the trip-instance key. Final outcomes
(reconcile) are never overwritten by provisional ones (stream).
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ghostbus_core.archive_layout import hour_prefixes, parse_heartbeat_key, parse_snapshot_key
from ghostbus_core.coverage import collection_gaps
from ghostbus_core.feeds import Feed
from ghostbus_core.lake import table_properties
from ghostbus_core.outcomes import DEFAULT, Evidence, Params, classify
from ghostbus_core.timetable import ID_SWITCH_GRACE
from ghostbus_jobs.stream import (
    StreamConfig,
    heartbeats_frame,
    trip_updates_frame,
    vehicle_positions_frame,
)
from ghostbus_jobs.timetable_load import DIM, Dim

if TYPE_CHECKING:
    from pyspark.sql._typing import UserDefinedFunctionLike  # stub only

GOLD = "lake.gold.trip_outcomes"
EARTH_RADIUS_M = 6_371_000.0


@dataclass(frozen=True)
class Sources:
    trip_updates: DataFrame
    vehicle_positions: DataFrame
    heartbeats: DataFrame
    name: str


# --- sources ------------------------------------------------------------------------------------


def sources_from_silver(
    spark: SparkSession, cfg: StreamConfig, start: datetime, end: datetime
) -> Sources:
    lo, hi = F.lit(start.replace(tzinfo=None)), F.lit(end.replace(tzinfo=None))
    return Sources(
        spark.table(cfg.silver("trip_updates")).where(F.col("fetched_at").between(lo, hi)),
        spark.table(cfg.silver("vehicle_positions")).where(F.col("fetched_at").between(lo, hi)),
        spark.table(cfg.silver("heartbeats")).where(F.col("fetched_at").between(lo, hi)),
        "silver",
    )


def _archive_keys(
    archive_uri: str, s3_endpoint: str | None, start: datetime, end: datetime
) -> list[str]:
    from ghostbus_ingest.archive import archive_from_uri

    archive = archive_from_uri(archive_uri, s3_endpoint)
    lo, hi = start.timestamp() * 1000, end.timestamp() * 1000
    keys = []
    for feed in ("trip_updates", "vehicles", "heartbeat"):
        for prefix in hour_prefixes(feed, start, end):
            for key in archive.list_keys(prefix):
                try:
                    _, ms = (
                        parse_heartbeat_key(key) if feed == "heartbeat" else parse_snapshot_key(key)
                    )
                except ValueError:
                    continue
                if lo <= ms <= hi:
                    keys.append(key)
    return keys


def _load_partition(
    keys: Iterable[str], archive_uri: str, s3_endpoint: str | None, topics: dict[str, str]
) -> Iterator[tuple[str, str]]:
    """Executor side: read archived objects and emit (topic, JSON) exactly as the poller did."""
    from ghostbus_core.archive_layout import fetch_id
    from ghostbus_core.gtfsr import FetchContext, normalize, parse_feed
    from ghostbus_ingest.archive import archive_from_uri, decompress

    archive = archive_from_uri(archive_uri, s3_endpoint)
    for key in keys:
        data = archive.get_bytes(key)
        if data is None:
            continue
        if key.endswith(".json"):
            yield topics["heartbeat"], data.decode("utf-8")
            continue
        feed, ms = parse_snapshot_key(key)
        try:
            msg = parse_feed(decompress(data))
        except Exception:
            continue  # the heartbeat of that call already says bad_payload
        ctx = FetchContext(feed=feed, fetch_id=fetch_id(feed, ms), fetched_at_ms=ms)
        topic = topics["trip_updates"] if feed is Feed.TRIP_UPDATES else topics["vehicle_positions"]
        for rec in normalize(ctx, msg).records:
            yield topic, json.dumps(rec.value, separators=(",", ":"), ensure_ascii=False)


def sources_from_archive(
    spark: SparkSession,
    archive_uri: str,
    s3_endpoint: str | None,
    start: datetime,
    end: datetime,
    cfg: StreamConfig | None = None,
) -> Sources:
    cfg = cfg or StreamConfig()
    keys = _archive_keys(archive_uri, s3_endpoint, start, end)
    topics = {
        "trip_updates": cfg.topics.trip_updates,
        "vehicle_positions": cfg.topics.vehicle_positions,
        "heartbeat": cfg.topics.heartbeat,
    }
    rdd = spark.sparkContext.parallelize(keys, max(1, min(64, len(keys) // 20))).mapPartitions(
        lambda it: _load_partition(it, archive_uri, s3_endpoint, topics)
    )
    raw = spark.createDataFrame(rdd, "topic STRING, value STRING").cache()
    return Sources(
        trip_updates_frame(raw, cfg),
        vehicle_positions_frame(raw, cfg),
        heartbeats_frame(raw, cfg),
        "archive",
    )


# --- instances and realtime matching ------------------------------------------------------------


def instances(spark: SparkSession, first: date, last: date, dim: Dim = DIM) -> DataFrame:
    parts = F.split("route_key", r"\|")
    return (
        spark.table(dim.trip_instance)
        .where(F.col("service_date").between(F.lit(first), F.lit(last)))
        .withColumn("agency_id", parts[0])
        .withColumn("route_type", parts[2])
    )


def with_trip_version(
    rt: DataFrame, spark: SparkSession, dim: Dim = DIM, grace: timedelta = ID_SWITCH_GRACE
) -> DataFrame:
    """Add ``feed_version``: the timetable version each realtime row's ``trip_id`` belongs to.

    The same rule as ``ghostbus_core.timetable.versions_for_realtime``: the version live at
    ``fetched_at``; within ``grace`` of its publish, the previous version if the id is only there.
    Adds ``version_fallback`` (true when the previous version was used) and ``version_found``.
    Rows are kept even when no version has the id (``feed_version`` is then the live one), so
    callers decide what to drop.
    """
    w = Window.orderBy("valid_from")
    fv = F.broadcast(
        spark.table(dim.feed_version)
        .select("feed_version", "valid_from", "valid_to")
        .withColumn("prev_version", F.lag("feed_version").over(w))
    )
    live = rt.drop("feed_version").join(
        fv,
        (F.col("fetched_at") >= F.col("valid_from"))
        & (F.col("valid_to").isNull() | (F.col("fetched_at") < F.col("valid_to"))),
        "left",
    )
    in_grace = F.col("prev_version").isNotNull() & (
        F.col("fetched_at")
        < F.col("valid_from") + F.expr(f"INTERVAL {int(grace.total_seconds())} SECONDS")
    )
    # Two deterministic left joins (trip_id is unique within a version, so no row is repeated).
    # Deterministic matters: the result feeds a MERGE, which rejects non-deterministic inputs
    # (a first version using monotonically_increasing_id failed exactly there).
    in_live = spark.table(dim.trip).select(
        F.col("feed_version").alias("_v1"), F.col("trip_id").alias("_t1")
    )
    in_prev = spark.table(dim.trip).select(
        F.col("feed_version").alias("_v2"), F.col("trip_id").alias("_t2")
    )
    joined = live.join(
        in_live,
        (F.col("feed_version") == F.col("_v1")) & (F.col("trip_id") == F.col("_t1")),
        "left",
    ).join(
        in_prev,
        in_grace & (F.col("prev_version") == F.col("_v2")) & (F.col("trip_id") == F.col("_t2")),
        "left",
    )
    found_live, found_prev = F.col("_v1").isNotNull(), F.col("_v2").isNotNull()
    return (
        joined.withColumn("version_fallback", ~found_live & found_prev)
        .withColumn("version_found", found_live | found_prev)
        .withColumn(
            "feed_version",
            F.when(found_live, F.col("feed_version"))
            .when(found_prev, F.col("prev_version"))
            .otherwise(F.col("feed_version")),
        )
        .drop("_v1", "_t1", "_v2", "_t2", "valid_from", "valid_to", "prev_version")
    )


def attach_instance(rt: DataFrame, spark: SparkSession, dim: Dim = DIM) -> DataFrame:
    """Realtime rows (``fetched_at``, ``trip_id``, ``start_date``) -> ``trip_instance_key``.

    The ``trip_id`` is looked up in the version live at fetch time (ADR-0006), or just after a
    publish in the previous one (``with_trip_version``), giving the trip's natural key, which then
    finds the scheduled instance on ``start_date`` even if that day's schedule came from another
    version. Among identical twins the same ``trip_id`` decides.
    """
    live = with_trip_version(rt.where(F.col("trip_id").isNotNull()), spark, dim)
    trips = spark.table(dim.trip).select(
        "feed_version",
        "trip_id",
        F.col("route_key").alias("t_route_key"),
        F.col("direction_id").alias("t_direction_id"),
        F.col("first_departure_s").alias("t_first_departure_s"),
        F.col("first_stop_id").alias("t_first_stop_id"),
    )
    keyed = live.join(trips, ["feed_version", "trip_id"])
    inst = spark.table(dim.trip_instance).select(
        F.col("service_date").alias("i_date"),
        "trip_instance_key",
        F.col("feed_version").alias("i_version"),
        F.col("trip_id").alias("i_trip_id"),
        F.col("route_key").alias("i_route_key"),
        F.col("direction_id").alias("i_direction_id"),
        F.col("first_departure_s").alias("i_first_departure_s"),
        F.col("first_stop_id").alias("i_first_stop_id"),
    )
    cond = (
        (F.to_date("start_date", "yyyyMMdd") == F.col("i_date"))
        & (F.col("t_route_key") == F.col("i_route_key"))
        & (F.col("t_direction_id") == F.col("i_direction_id"))
        & (F.col("t_first_departure_s") == F.col("i_first_departure_s"))
        & (F.col("t_first_stop_id") == F.col("i_first_stop_id"))
        & ((F.col("i_version") != F.col("feed_version")) | (F.col("i_trip_id") == F.col("trip_id")))
    )
    return keyed.join(inst, cond)


# --- evidence -----------------------------------------------------------------------------------


def _haversine_m(lat1: str, lon1: str, lat2: str, lon2: str) -> Column:
    dlat = F.radians(F.col(lat2) - F.col(lat1))
    dlon = F.radians(F.col(lon2) - F.col(lon1))
    a = (
        F.sin(dlat / 2) ** 2
        + F.cos(F.radians(F.col(lat1))) * F.cos(F.radians(F.col(lat2))) * F.sin(dlon / 2) ** 2
    )
    return 2 * EARTH_RADIUS_M * F.asin(F.sqrt(a))


def until_close(rows: DataFrame, inst: DataFrame, p: Params) -> DataFrame:
    """Keep only realtime rows fetched by their trip's close time.

    A trip's evidence is then fixed once it closes, whenever and from whichever source it is
    computed. Without this, a late bus still in the feeds was counted up to "now", so the archive
    (one poll ahead of the stream) and the stream disagreed on the same trip.
    """
    closes = inst.select(
        "trip_instance_key",
        (F.col("scheduled_end") + F.expr(f"INTERVAL {p.close_after_min} MINUTES")).alias("_closes"),
    )
    return (
        rows.join(closes, "trip_instance_key")
        .where(F.col("fetched_at") <= F.col("_closes"))
        .drop("_closes")
    )


def trip_update_evidence(
    src: Sources, spark: SparkSession, inst: DataFrame, p: Params, dim: Dim = DIM
) -> DataFrame:
    return (
        until_close(attach_instance(src.trip_updates, spark, dim), inst, p)
        .groupBy("trip_instance_key")
        .agg(
            F.countDistinct("fetch_id").alias("tu_snapshots"),
            F.min(F.when(F.col("schedule_relationship") == "CANCELED", F.col("fetched_at"))).alias(
                "cancelled_at"
            ),
            F.max(F.col("vehicle_id").isNotNull()).alias("tu_vehicle"),
        )
    )


def vehicle_evidence(
    src: Sources, spark: SparkSession, inst: DataFrame, p: Params, dim: Dim = DIM
) -> DataFrame:
    """Positions per trip, and progress: the furthest stop any position came within range of."""
    positions = until_close(attach_instance(src.vehicle_positions, spark, dim), inst, p).select(
        "trip_instance_key", "event_time", "latitude", "longitude"
    )
    ordinal = Window.partitionBy("feed_version", "trip_id").orderBy("stop_sequence")
    stop_times = spark.table(dim.stop_time).withColumn("ordinal", F.row_number().over(ordinal))
    stops = (
        spark.table(dim.stop)
        .where("is_current")
        .select(
            "stop_id",
            F.col("stop_lat").cast("double").alias("s_lat"),
            F.col("stop_lon").cast("double").alias("s_lon"),
        )
    )
    route = (
        inst.select("trip_instance_key", "feed_version", "trip_id", "n_stops")
        .join(stop_times, ["feed_version", "trip_id"])
        .join(stops, "stop_id")
        .select("trip_instance_key", "ordinal", "n_stops", "s_lat", "s_lon")
    )
    reached = (
        positions.join(route, "trip_instance_key")
        .where(_haversine_m("latitude", "longitude", "s_lat", "s_lon") <= p.stop_radius_m)
        .groupBy("trip_instance_key")
        .agg((F.max("ordinal") / F.max("n_stops")).alias("progress"))
    )
    counts = positions.groupBy("trip_instance_key").agg(
        F.countDistinct("event_time").alias("vp_positions"),
        F.min("event_time").alias("first_vp"),
        F.max("event_time").alias("last_vp"),
    )
    return counts.join(reached, "trip_instance_key", "left").fillna({"progress": 0.0})


def coverage_gaps(
    src: Sources, start: datetime, end: datetime, p: Params
) -> dict[str, list[tuple[datetime, datetime]]]:
    """Collection gaps per feed, from the heartbeats of the same source (computed in the driver)."""
    rows = src.heartbeats.where("collected").select("feed", "fetched_at").distinct().collect()
    out = {}
    for feed in ("trip_updates", "vehicles"):
        times = [r["fetched_at"].replace(tzinfo=UTC) for r in rows if r["feed"] == feed]
        out[feed] = collection_gaps(times, start, end, timedelta(minutes=p.max_collection_gap_min))
    return out


def _gap_frame(spark: SparkSession, gaps: list[tuple[datetime, datetime]]) -> DataFrame:
    schema = StructType(
        [StructField("g0", TimestampType(), False), StructField("g1", TimestampType(), False)]
    )
    return spark.createDataFrame(
        [(a.replace(tzinfo=None), b.replace(tzinfo=None)) for a, b in gaps], schema
    )


# --- classification -------------------------------------------------------------------------------

_RESULT = StructType([StructField("outcome", StringType()), StructField("reason", StringType())])


def _classify_udf(p: Params) -> UserDefinedFunctionLike:
    def run(
        route_type: str,
        scheduled_start: datetime,
        cancelled_at: datetime | None,
        tu_snapshots: int,
        tu_vehicle: bool,
        vp_positions: int,
        progress: float | None,
        tu_gap: bool,
        vp_gap: bool,
    ) -> tuple[str, str]:
        o, reason = classify(
            Evidence(
                route_type=route_type,
                scheduled_start=scheduled_start.replace(tzinfo=UTC),
                cancelled_at=cancelled_at.replace(tzinfo=UTC) if cancelled_at else None,
                tu_snapshots=tu_snapshots,
                tu_vehicle=tu_vehicle,
                vp_positions=vp_positions,
                progress=progress,
                tu_gap=tu_gap,
                vp_gap=vp_gap,
            ),
            p,
        )
        return o.value, reason

    return F.udf(run, _RESULT)


def outcomes(
    spark: SparkSession,
    src: Sources,
    first: date,
    last: date,
    clock: datetime,
    status: str,
    p: Params = DEFAULT,
    dim: Dim = DIM,
    horizon_start: datetime | None = None,
    closed_after: datetime | None = None,
) -> DataFrame:
    """Outcomes for every trip on ``first..last`` that is closed by ``clock`` (the data clock).

    ``horizon_start`` is when the evidence begins; anything before it counts as a gap.
    """
    clock_naive = clock.replace(tzinfo=None)
    closes = F.col("scheduled_end") + F.expr(f"INTERVAL {p.close_after_min} MINUTES")
    inst = instances(spark, first, last, dim).where(closes <= F.lit(clock_naive))
    if closed_after is not None:
        inst = inst.where(closes > F.lit(closed_after.replace(tzinfo=None)))
    window_lo = inst.agg(F.min("scheduled_start")).first()
    if window_lo is None or window_lo[0] is None:
        return (
            spark.createDataFrame([], spark.table(GOLD).schema)
            if spark.catalog.tableExists(GOLD)
            else inst.limit(0)
        )
    start = window_lo[0].replace(tzinfo=UTC) - timedelta(minutes=p.window_before_min + 60)
    if horizon_start is not None:
        start = min(start, horizon_start)
    gaps = coverage_gaps(src, horizon_start or start, clock, p)

    tu = trip_update_evidence(src, spark, inst, p, dim)
    vp = vehicle_evidence(src, spark, inst, p, dim)
    before = F.expr(f"INTERVAL {p.window_before_min} MINUTES")
    after = F.expr(f"INTERVAL {p.window_after_min} MINUTES")
    ev = (
        inst.join(tu, "trip_instance_key", "left")
        .join(vp, "trip_instance_key", "left")
        .fillna({"tu_snapshots": 0, "tu_vehicle": False, "vp_positions": 0})
        .withColumn("w0", F.col("scheduled_start") - before)
        .withColumn(
            "w_tu1",
            F.when(
                F.col("cancelled_at").isNotNull(), F.greatest("cancelled_at", F.col("w0"))
            ).otherwise(F.col("scheduled_end") + after),
        )
        .withColumn("w_vp1", F.col("scheduled_end") + after)
    )
    for feed, col, end_col in (
        ("trip_updates", "tu_gap", "w_tu1"),
        ("vehicles", "vp_gap", "w_vp1"),
    ):
        g = F.broadcast(_gap_frame(spark, gaps[feed]))
        hit = (
            ev.select("trip_instance_key", "w0", end_col)
            .join(g, (F.col("g0") < F.col(end_col)) & (F.col("w0") < F.col("g1")))
            .select("trip_instance_key")
            .distinct()
            .withColumn(col, F.lit(True))
        )
        ev = ev.join(hit, "trip_instance_key", "left").fillna({col: False})

    result = _classify_udf(p)(
        "route_type",
        "scheduled_start",
        "cancelled_at",
        "tu_snapshots",
        "tu_vehicle",
        "vp_positions",
        "progress",
        "tu_gap",
        "vp_gap",
    )
    return ev.withColumn("r", result).select(
        "service_date",
        "trip_instance_key",
        "agency_id",
        "route_key",
        "route_type",
        "direction_id",
        "scheduled_start",
        "scheduled_end",
        "n_stops",
        F.col("r.outcome").alias("outcome"),
        F.col("r.reason").alias("reason"),
        F.col("tu_snapshots").cast("int").alias("tu_snapshots"),
        "cancelled_at",
        F.col("tu_vehicle").cast(BooleanType()).alias("tu_vehicle"),
        F.col("vp_positions").cast("int").alias("vp_positions"),
        "first_vp",
        "last_vp",
        F.col("progress").cast("double").alias("progress"),
        "tu_gap",
        "vp_gap",
        F.lit(status).alias("status"),
        F.lit(src.name).alias("source"),
        F.lit(p.version).alias("params_version"),
        F.lit(clock_naive).cast("timestamp").alias("data_clock"),
    )


# --- gold -----------------------------------------------------------------------------------------


def ensure_gold(spark: SparkSession, df: DataFrame, table: str = GOLD) -> None:
    props = ", ".join(f"'{k}'='{v}'" for k, v in table_properties().items())
    cols = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in df.schema.fields)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {table.rsplit('.', 1)[0]}")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} ({cols}) USING iceberg "
        f"PARTITIONED BY (service_date) TBLPROPERTIES ({props})"
    )


def merge_outcomes(spark: SparkSession, df: DataFrame, table: str = GOLD) -> None:
    """Upsert by trip-instance key. A final outcome is never replaced by a provisional one, and
    an identical row is left alone, so replays are no-ops."""
    ensure_gold(spark, df, table)
    df.createOrReplaceTempView("new_outcomes")
    spark.sql(f"""
        MERGE INTO {table} t
        USING new_outcomes s
        ON t.service_date = s.service_date AND t.trip_instance_key = s.trip_instance_key
        WHEN MATCHED AND (t.status = 'provisional' OR s.status = 'final')
             AND (t.outcome <> s.outcome OR t.status <> s.status OR t.source <> s.source
                  OR t.params_version <> s.params_version
                  OR t.reason <> s.reason) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
