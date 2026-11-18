"""timetable_load on two NTA-shaped versions, in a throwaway namespace of the local lake.

The Sprint 2 promise: a trip from last week still resolves to last week's route name and stops,
even though NTA regenerated every id in between.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ghostbus_core.lake import LakeSettings
from ghostbus_core.timetable import (
    format_gtfs_time,
    gtfs_time_to_utc,
    order_versions,
    parse_gtfs_time,
    scd2_apply,
)
from ghostbus_ingest.archive import LocalArchive
from ghostbus_ingest.static_archive import extract, list_versions
from ghostbus_jobs.spark import build_spark
from ghostbus_jobs.timetable_load import (
    ROUTE_ATTRS,
    Dim,
    build_trip_instances,
    gtfs_seconds,
    gtfs_time_text,
    load_versions,
    route_snapshot,
    run,
)
from tests.gtfs_builder import V1_PUBLISHED, V2_PUBLISHED, archive_files, archive_version, version

pytestmark = pytest.mark.integration

V1_DAY = date(2026, 11, 12)  # a Thursday under version 1
V2_DAY = date(2026, 11, 16)  # a Monday under version 2
DST_DAY = date(2027, 3, 28)  # clocks go forward


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    import os

    session = build_spark("it-timetable", LakeSettings.from_env(dict(os.environ)))
    yield session


@pytest.fixture(scope="module")
def loaded(
    spark: SparkSession, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[tuple[Dim, LocalArchive, Path]]:
    tmp = tmp_path_factory.mktemp("timetable")
    archive = LocalArchive(tmp / "archive")
    archive_version(archive, 1, V1_PUBLISHED, tmp)
    archive_version(archive, 2, V2_PUBLISHED, tmp)
    dim = Dim(f"lake.it_{uuid.uuid4().hex[:8]}")
    run(spark, archive, tmp / "work", V1_DAY, V2_DAY, dim=dim)
    build_trip_instances(spark, sorted_versions(archive), DST_DAY, DST_DAY, dim)
    yield dim, archive, tmp
    for t in dim.all():
        spark.sql(f"DROP TABLE IF EXISTS {t} PURGE")
    spark.sql(f"DROP NAMESPACE IF EXISTS {dim.ns}")


def sorted_versions(archive: LocalArchive) -> list:  # type: ignore[type-arg]
    return order_versions(a.version for a in list_versions(archive))


def utc(ts: datetime) -> datetime:
    return ts.replace(tzinfo=UTC)


def test_feed_versions_chain(spark: SparkSession, loaded: tuple[Dim, LocalArchive, Path]) -> None:
    dim, _, _ = loaded
    rows = spark.table(dim.feed_version).orderBy("valid_from").collect()
    assert [r["feed_version"].rsplit("-", 1)[0] for r in rows] == ["VERSION-1", "VERSION-2"]
    assert all(r["feed_version"].endswith("-" + r["sha256"][:8]) for r in rows)
    assert utc(rows[0]["valid_to"]) == V2_PUBLISHED and not rows[0]["is_current"]
    assert rows[1]["valid_to"] is None and rows[1]["is_current"]


def test_routes_survive_id_regeneration(
    spark: SparkSession, loaded: tuple[Dim, LocalArchive, Path]
) -> None:
    dim, _, _ = loaded
    rows = spark.table(dim.route).collect()
    by_key: dict[str, list] = {}  # type: ignore[type-arg]
    for r in rows:
        by_key.setdefault(r["route_key"], []).append(r)
    # Unchanged routes: one row each, although every route_id changed.
    assert len(by_key["IR|rail|2|Dublin - Galway"]) == 1
    assert len(by_key["10000|Green|0"]) == 1
    # Renamed: old row closed at the new version's publish time, new row current.
    a46 = sorted(by_key["1|46A|3"], key=lambda r: r["valid_from"])
    assert [r["route_long_name"] for r in a46] == [
        "Phoenix Park - Dun Laoghaire",
        "Phoenix Park - Dún Laoghaire",
    ]
    assert utc(a46[0]["valid_to"]) == V2_PUBLISHED and a46[1]["is_current"]
    # Withdrawn and new.
    (r39,) = by_key["1|39|3"]
    assert not r39["is_current"] and utc(r39["valid_to"]) == V2_PUBLISHED
    (r7,) = by_key["1|7|3"]
    assert r7["is_current"] and r7["feed_version"].startswith("VERSION-2-")


def test_spark_scd2_equals_the_reference(
    spark: SparkSession, loaded: tuple[Dim, LocalArchive, Path]
) -> None:
    dim, archive, tmp = loaded
    history: list = []  # type: ignore[type-arg]
    for a in list_versions(archive):
        snapshot, _, _ = route_snapshot(extract(archive, a, tmp / "ref"))
        history = scd2_apply(history, snapshot, a.version)
    expected = {
        (r.key, r.valid_from, r.valid_to, tuple(str(r.attrs[c]) for c in ROUTE_ATTRS))
        for r in history
    }
    actual = {
        (
            r["route_key"],
            utc(r["valid_from"]),
            utc(r["valid_to"]) if r["valid_to"] else None,
            tuple(r[c] or "" for c in ROUTE_ATTRS),
        )
        for r in spark.table(dim.route).collect()
    }
    assert actual == expected


def test_old_trips_resolve_to_old_definitions(
    spark: SparkSession, loaded: tuple[Dim, LocalArchive, Path]
) -> None:
    """Point-in-time join: each trip instance sees the route and stop rows valid when it ran."""
    dim, _, _ = loaded
    spark.table(dim.trip_instance).createOrReplaceTempView("ti")
    spark.table(dim.route).createOrReplaceTempView("r")
    spark.table(dim.stop).createOrReplaceTempView("s")
    rows = spark.sql("""
        SELECT ti.service_date, ti.route_key, r.route_long_name, s.stop_name AS first_stop_name
        FROM ti
        JOIN r ON r.route_key = ti.route_key
              AND ti.scheduled_start >= r.valid_from
              AND (r.valid_to IS NULL OR ti.scheduled_start < r.valid_to)
        LEFT JOIN s ON s.stop_id = ti.first_stop_id
              AND ti.scheduled_start >= s.valid_from
              AND (s.valid_to IS NULL OR ti.scheduled_start < s.valid_to)
        WHERE ti.service_date IN (DATE '2026-11-12', DATE '2026-11-16')
    """).collect()
    names = {(r["service_date"], r["route_key"]): r["route_long_name"] for r in rows}
    assert names[(V1_DAY, "1|46A|3")] == "Phoenix Park - Dun Laoghaire"
    assert names[(V2_DAY, "1|46A|3")] == "Phoenix Park - Dún Laoghaire"
    assert names[(V1_DAY, "1|39|3")] == "Ongar - Burlington Road"  # route no longer exists
    assert (V2_DAY, "1|39|3") not in names
    luas = {
        (r["service_date"], r["first_stop_name"]) for r in rows if r["route_key"] == "10000|Green|0"
    }
    assert luas == {(V1_DAY, "Merrion Sq"), (V2_DAY, "Merrion Square")}
    instances = (
        spark.table(dim.trip_instance).where(F.col("service_date").isin(V1_DAY, V2_DAY)).count()
    )
    assert len(rows) == instances  # every instance found exactly one route row


def test_trip_instance_keys_are_stable_across_versions(
    spark: SparkSession, loaded: tuple[Dim, LocalArchive, Path]
) -> None:
    dim, _, _ = loaded
    rows = spark.table(dim.trip_instance).collect()

    def keys(day: date) -> set[str]:
        return {
            r["trip_instance_key"].replace(f"{day:%Y%m%d}", "DATE")
            for r in rows
            if r["service_date"] == day
        }

    common = keys(V1_DAY) & keys(V2_DAY)
    assert "1|46A|3|0|DATE|08:00:00|S1" in common
    assert "10000|Green|0|1|DATE|10:25:00|S2|1" in common  # the duplicate index is stable too
    v1_ids = {r["trip_id"] for r in rows if r["service_date"] == V1_DAY}
    v2_ids = {r["trip_id"] for r in rows if r["service_date"] == V2_DAY}
    assert not v1_ids & v2_ids  # ...while every NTA trip_id changed


def test_times_after_midnight_and_on_clock_change(
    spark: SparkSession, loaded: tuple[Dim, LocalArchive, Path]
) -> None:
    dim, _, _ = loaded
    rows = {
        (r["service_date"], r["first_departure_s"]): r
        for r in spark.table(dim.trip_instance).collect()
    }
    night = rows[(V1_DAY, parse_gtfs_time("24:30:00"))]
    assert utc(night["scheduled_start"]) == datetime(2026, 11, 13, 0, 30, tzinfo=UTC)
    dst = rows[(DST_DAY, parse_gtfs_time("08:15:00"))]
    assert utc(dst["scheduled_start"]) == gtfs_time_to_utc(DST_DAY, parse_gtfs_time("08:15:00"))
    assert utc(dst["scheduled_start"]) == datetime(2027, 3, 28, 7, 15, tzinfo=UTC)  # 08:15 IST


def test_rerun_changes_nothing(spark: SparkSession, loaded: tuple[Dim, LocalArchive, Path]) -> None:
    dim, archive, tmp = loaded
    before = {
        t: sorted(map(str, spark.table(t).drop("loaded_at").collect()))
        for t in dim.all()
        if not t.endswith("feed_version")
    }
    run(spark, archive, tmp / "work", V1_DAY, V2_DAY, dim=dim)
    build_trip_instances(spark, sorted_versions(archive), DST_DAY, DST_DAY, dim)
    after = {t: sorted(map(str, spark.table(t).drop("loaded_at").collect())) for t in before}
    assert after == before


@pytest.mark.parametrize("text", ["00:00:00", "08:15:30", "24:30:00", "32:38:00"])
def test_spark_time_functions_match_core(spark: SparkSession, text: str) -> None:
    row = (
        spark.createDataFrame([(text,)], "t STRING")
        .select(
            gtfs_seconds(F.col("t")).alias("s"),
            gtfs_time_text(gtfs_seconds(F.col("t"))).alias("back"),
        )
        .first()
    )
    assert row is not None
    assert row["s"] == parse_gtfs_time(text)
    assert row["back"] == format_gtfs_time(parse_gtfs_time(text))


def test_a_republish_with_the_same_label_keeps_earlier_days(
    spark: SparkSession, tmp_path: Path
) -> None:
    """NTA re-uploads nightly, sometimes with the same feed_version label and different contents.
    Here the republish (same label, calendar from 13 Nov, published 12 Nov at noon, after that
    day's 04:00 cut-off) must not replace the trips that defined 12 Nov. Keyed by label it did,
    and the day lost all its trips; on real data, 15 Nov 2026 lost 5,182 of 12,185."""
    archive = LocalArchive(tmp_path / "archive")
    archive_version(archive, 1, V1_PUBLISHED, tmp_path)
    republished = version(1)
    republished["calendar.txt"] = [
        {**row, "start_date": "20261113"} for row in republished["calendar.txt"]
    ]
    archive_files(archive, republished, datetime(2026, 11, 12, 12, 0, tzinfo=UTC), tmp_path)
    dim = Dim(f"lake.it_{uuid.uuid4().hex[:8]}")
    try:
        run(spark, archive, tmp_path / "work", V1_DAY, V1_DAY, dim=dim)
        versions = spark.table(dim.feed_version).collect()
        assert len({r["feed_version"] for r in versions}) == 2
        assert {r["feed_version"].rsplit("-", 1)[0] for r in versions} == {"VERSION-1"}
        day = spark.table(dim.trip_instance).where(F.col("service_date") == V1_DAY).count()
        expected = (
            spark.table(dim.trip_instance)
            .where(F.col("service_date") == V1_DAY)
            .select("feed_version")
            .distinct()
            .collect()
        )
        assert day > 0, "12 Nov lost its trips to the same-label republish"
        assert [r["feed_version"] for r in expected] == [
            min(versions, key=lambda r: r["valid_from"])["feed_version"]
        ]
    finally:
        for t in dim.all():
            spark.sql(f"DROP TABLE IF EXISTS {t} PURGE")
        spark.sql(f"DROP NAMESPACE IF EXISTS {dim.ns}")


def test_rebuild_empties_and_reloads_to_the_same_result(
    spark: SparkSession, loaded: tuple[Dim, LocalArchive, Path]
) -> None:
    dim, archive, tmp = loaded
    before = {t: spark.table(t).count() for t in dim.all() if not t.endswith("trip_instance")}
    load_versions(spark, archive, tmp / "work", rebuild=True, dim=dim)
    after = {t: spark.table(t).count() for t in dim.all() if not t.endswith("trip_instance")}
    assert after == before and all(n > 0 for n in after.values())
