from __future__ import annotations

import itertools
from datetime import UTC, date, datetime, timedelta

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from ghostbus_core.timetable import (
    LOCAL_TZ,
    FeedVersion,
    Scd2Row,
    format_gtfs_time,
    gtfs_time_to_utc,
    parse_gtfs_date,
    parse_gtfs_time,
    resolve,
    route_key,
    row_hash,
    scd2_apply,
    schedule_version_for,
    service_dates,
    service_day_origin,
    trip_instance_key,
    version_live_at,
)

dates = st.dates(min_value=date(2024, 1, 1), max_value=date(2032, 12, 31))

# --- keys -----------------------------------------------------------------------------------------


def test_bus_route_key_ignores_long_name() -> None:
    assert route_key("1", "46A", "3", "Phoenix Park - Dun Laoghaire") == "1|46A|3"


def test_rail_route_key_uses_long_name() -> None:
    """Irish Rail names every route 'rail'; without the long name they would all merge."""
    galway = route_key("IR", "rail", "2", "Dublin - Galway")
    sligo = route_key("IR", "rail", "2", "Dublin - Sligo")
    assert galway == "IR|rail|2|Dublin - Galway" and galway != sligo


def test_route_key_needs_agency() -> None:
    with pytest.raises(ValueError):
        route_key("", "46A", "3")


def test_trip_instance_key() -> None:
    base = trip_instance_key("1|46A|3", "0", date(2026, 11, 16), 25 * 3600 + 5, "8220DB000002")
    assert base == "1|46A|3|0|20261116|25:00:05|8220DB000002"
    assert trip_instance_key("k", "1", date(2026, 11, 16), 0, "s", duplicate_index=1).endswith("|1")


# --- times ----------------------------------------------------------------------------------------


@given(st.integers(min_value=0, max_value=48 * 3600))
def test_gtfs_time_round_trip(seconds: int) -> None:
    assert parse_gtfs_time(format_gtfs_time(seconds)) == seconds


@pytest.mark.parametrize("bad", ["", "7:00", "07:60:00", "07:00:61", "aa:bb:cc", "-1:00:00"])
def test_bad_gtfs_times(bad: str) -> None:
    with pytest.raises(ValueError):
        parse_gtfs_time(bad)


def test_parse_gtfs_date() -> None:
    assert parse_gtfs_date("20271114") == date(2027, 11, 14)
    with pytest.raises(ValueError):
        parse_gtfs_date("2027-11-14")


def test_origin_is_local_midnight_on_normal_days() -> None:
    assert service_day_origin(date(2026, 11, 16)) == datetime(2026, 11, 16, 0, tzinfo=UTC)  # GMT
    assert service_day_origin(date(2026, 7, 1)) == datetime(2026, 6, 30, 23, tzinfo=UTC)  # IST


def test_origin_on_spring_clock_change_is_the_evening_before() -> None:
    """28 March 2027, in the current NTA timetable: clocks go forward at 01:00 UTC."""
    assert service_day_origin(date(2027, 3, 28)) == datetime(2027, 3, 27, 23, tzinfo=UTC)


def test_origin_on_autumn_clock_change_is_one_am() -> None:
    assert service_day_origin(date(2026, 10, 25)) == datetime(2026, 10, 25, 0, tzinfo=UTC)


def test_after_midnight_trip_lands_on_the_next_morning() -> None:
    at = gtfs_time_to_utc(date(2026, 11, 15), parse_gtfs_time("25:30:00"))
    assert at.astimezone(LOCAL_TZ) == datetime(2026, 11, 16, 1, 30, tzinfo=LOCAL_TZ)


@given(dates, st.integers(min_value=4 * 3600, max_value=24 * 3600 - 1))
def test_daytime_gtfs_times_match_the_wall_clock_every_day(d: date, seconds: int) -> None:
    """From 04:00 to midnight, '08:15:00' means 08:15 on the clock, clock change or not."""
    local = gtfs_time_to_utc(d, seconds).astimezone(LOCAL_TZ)
    assert local.date() == d
    assert local.hour * 3600 + local.minute * 60 + local.second == seconds


@given(dates)
def test_days_are_23_24_or_25_hours_apart(d: date) -> None:
    gap = service_day_origin(d + timedelta(days=1)) - service_day_origin(d)
    assert gap in {timedelta(hours=23), timedelta(hours=24), timedelta(hours=25)}


@given(dates, st.integers(0, 40 * 3600), st.integers(1, 3600))
def test_later_gtfs_times_are_later_instants(d: date, s: int, delta: int) -> None:
    assert gtfs_time_to_utc(d, s + delta) - gtfs_time_to_utc(d, s) == timedelta(seconds=delta)


# --- calendars ------------------------------------------------------------------------------------

WEEKDAYS = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")


def cal(sid: str, days: str, start: str, end: str) -> dict[str, str]:
    row = {"service_id": sid, "start_date": start, "end_date": end}
    row |= {w: ("1" if c == "1" else "0") for w, c in zip(WEEKDAYS, days, strict=True)}
    return row


def test_weekly_pattern_with_exceptions() -> None:
    out = service_dates(
        [
            cal("weekdays", "1111100", "20261116", "20261129"),
            cal("never", "0000000", "20261226", "20261226"),
        ],
        [
            {"service_id": "weekdays", "date": "20261118", "exception_type": "2"},
            {"service_id": "weekdays", "date": "20261122", "exception_type": "1"},  # a Sunday
            {"service_id": "never", "date": "20261226", "exception_type": "1"},
            {"service_id": "dates_only", "date": "20270101", "exception_type": "1"},
        ],
    )
    assert date(2026, 11, 18) not in out["weekdays"]
    assert date(2026, 11, 22) in out["weekdays"]
    assert len(out["weekdays"]) == 10 - 1 + 1
    assert out["never"] == {date(2026, 12, 26)}
    assert out["dates_only"] == {date(2027, 1, 1)}


def test_removal_beats_addition() -> None:
    out = service_dates(
        [],
        [
            {"service_id": "s", "date": "20261201", "exception_type": "2"},
            {"service_id": "s", "date": "20261201", "exception_type": "1"},
        ],
    )
    assert out["s"] == set()


@settings(max_examples=200)
@given(
    start=st.dates(min_value=date(2026, 1, 1), max_value=date(2027, 12, 31)),
    length=st.integers(0, 60),
    pattern=st.text(alphabet="01", min_size=7, max_size=7),
    exceptions=st.lists(st.tuples(st.integers(-10, 70), st.sampled_from(["1", "2"])), max_size=10),
)
def test_calendar_matches_a_brute_force_oracle(
    start: date, length: int, pattern: str, exceptions: list[tuple[int, str]]
) -> None:
    end = start + timedelta(days=length)
    rows = [cal("s", pattern, f"{start:%Y%m%d}", f"{end:%Y%m%d}")]
    exc = [
        {"service_id": "s", "date": f"{start + timedelta(days=o):%Y%m%d}", "exception_type": k}
        for o, k in exceptions
    ]
    expected = {start + timedelta(days=i) for i in range(length + 1)}
    expected = {d for d in expected if pattern[d.weekday()] == "1"}
    expected |= {start + timedelta(days=o) for o, k in exceptions if k == "1"}
    expected -= {start + timedelta(days=o) for o, k in exceptions if k == "2"}
    assert service_dates(rows, exc)["s"] == expected


# --- versions -------------------------------------------------------------------------------------

V1 = FeedVersion("v1", "a" * 64, datetime(2026, 11, 1, 22, 0, tzinfo=UTC))
V2 = FeedVersion("v2", "b" * 64, datetime(2026, 11, 14, 23, 9, 2, tzinfo=UTC))  # real publish time


def test_version_live_at() -> None:
    assert version_live_at([V2, V1], datetime(2026, 10, 1, tzinfo=UTC)) is None
    assert version_live_at([V2, V1], datetime(2026, 11, 14, 23, 9, tzinfo=UTC)) == V1
    assert version_live_at([V2, V1], datetime(2026, 11, 14, 23, 9, 2, tzinfo=UTC)) == V2


def test_late_evening_publication_takes_over_the_next_service_day() -> None:
    assert schedule_version_for([V1, V2], date(2026, 11, 14)) == V1
    assert schedule_version_for([V1, V2], date(2026, 11, 15)) == V2


# --- SCD2 -----------------------------------------------------------------------------------------


def test_row_hash_ignores_key_order_and_padding() -> None:
    assert row_hash({"a": "x", "b": "y "}) == row_hash({"b": "y", "a": "x"})
    assert row_hash({"a": "x"}) != row_hash({"a": "z"})


def test_scd2_lifecycle_and_point_in_time_lookup() -> None:
    v3 = FeedVersion("v3", "c" * 64, datetime(2026, 12, 1, tzinfo=UTC))
    h = scd2_apply([], {"46A": {"name": "Phoenix Park"}, "39": {"name": "Ongar"}}, V1)
    h = scd2_apply(
        h, {"46A": {"name": "Phoenix Park - Dun Laoghaire"}, "39": {"name": "Ongar"}}, V2
    )
    h = scd2_apply(h, {"46A": {"name": "Phoenix Park - Dun Laoghaire"}}, v3)  # 39 withdrawn

    assert [r.attrs["name"] for r in h if r.key == "46A"] == [
        "Phoenix Park",
        "Phoenix Park - Dun Laoghaire",
    ]
    week_before = V2.valid_from - timedelta(days=7)
    old = resolve(h, "46A", week_before)
    assert old is not None and old.attrs["name"] == "Phoenix Park" and old.feed_version == "v1"
    new = resolve(h, "46A", V2.valid_from)
    assert new is not None and new.feed_version == "v2"
    assert resolve(h, "39", v3.valid_from) is None
    ongar = resolve(h, "39", week_before)
    assert ongar is not None and ongar.valid_to == v3.valid_from


def test_scd2_reapplying_a_version_is_a_no_op() -> None:
    h = scd2_apply([], {"k": {"a": 1}}, V1)
    assert scd2_apply(h, {"k": {"a": 1}}, V1) == h


def test_scd2_refuses_an_older_version() -> None:
    h = scd2_apply([], {"k": {"a": 1}}, V2)
    with pytest.raises(ValueError):
        scd2_apply(h, {"k": {"a": 2}}, V1)


snapshots = st.lists(
    st.dictionaries(st.sampled_from("ABCDE"), st.sampled_from(["x", "y", "z"]), max_size=5),
    min_size=1,
    max_size=8,
)


@settings(max_examples=200)
@given(snapshots)
def test_scd2_invariants_over_random_histories(snaps: list[dict[str, str]]) -> None:
    versions = [
        FeedVersion(f"v{i}", f"{i:064x}", datetime(2026, 1, 1, tzinfo=UTC) + timedelta(days=i))
        for i in range(len(snaps))
    ]
    history: list[Scd2Row] = []
    for snap, v in zip(snaps, versions, strict=True):
        history = scd2_apply(history, {k: {"value": val} for k, val in snap.items()}, v)

    for key in "ABCDE":
        rows = sorted((r for r in history if r.key == key), key=lambda r: r.valid_from)
        assert sum(r.is_current for r in rows) <= 1
        for a, b in itertools.pairwise(rows):
            assert a.valid_to is not None and a.valid_to <= b.valid_from  # no overlap
    # Every version, looked up at its own start, gives back exactly that snapshot.
    for snap, v in zip(snaps, versions, strict=True):
        for key in "ABCDE":
            r = resolve(history, key, v.valid_from)
            if key in snap:
                assert r is not None and r.attrs == {"value": snap[key]}
            else:
                assert r is None


def test_version_key_is_unique_per_zip_and_keeps_the_label() -> None:
    from ghostbus_core.timetable import version_key

    a = version_key("14FEE109", "f72084e211e0" + "0" * 52)
    b = version_key("14FEE109", "5a3116d6ac5e" + "0" * 52)
    assert a != b and a.startswith("14FEE109-") and a.endswith("f72084e2")
