"""Prediction accuracy (ADR-0016): passage times from positions, the board's prediction, errors by
lead time, and summaries that merge. Routes are synthetic, so every true time is known."""

from __future__ import annotations

import math

import jsonschema
import pytest
from hypothesis import given
from hypothesis import strategies as st

from ghostbus_core import schemas
from ghostbus_core.accuracy import (
    HIST_BINS,
    Acc,
    Fix,
    Route,
    Snapshot,
    StopTime,
    Update,
    accumulate,
    accuracy_doc,
    lead_bucket,
    passages,
    predicted,
    quantile,
    summary,
    trip_errors,
)

T0 = 1_794_816_000  # 18 Nov 2026 08:00 UTC
LAT0, LON0 = 53.30, -6.26
M_PER_DEG_LAT = math.pi / 180 * 6_371_000


def north(m: float) -> float:
    return LAT0 + m / M_PER_DEG_LAT


def line(n: int = 6, spacing: float = 500, every_s: int = 120) -> list[StopTime]:
    """Stops every 500 m due north, scheduled every 2 minutes from T0."""
    return [StopTime(i + 1, north(i * spacing), LON0, T0 + i * every_s) for i in range(n)]


def drive(speed_mps: float, start: int, until: int, step: int = 120, late: int = 0) -> list[Fix]:
    """A bus leaving stop 1 at ``start + late`` at constant speed, sampled every ``step`` s."""
    return [
        Fix(t, north(max(0.0, (t - start - late) * speed_mps)), LON0)
        for t in range(start, until, step)
    ]


def test_passage_times_are_interpolated_between_fixes() -> None:
    stops = line()
    fixes = drive(500 / 120, T0 - 60, T0 + 900, step=100)  # 4.17 m/s, fixes every 100 s
    got = passages(Route.of(stops), fixes)
    for s in stops[1:]:
        assert got[s.seq] == pytest.approx(T0 - 60 + (s.seq - 1) * 120, abs=1)


def test_no_time_across_a_long_gap_and_off_route_fixes_are_ignored() -> None:
    stops = line()
    fixes = [
        *drive(500 / 120, T0, T0 + 180, step=60),  # reaches about 750 m
        Fix(T0 + 500, north(1600), LON0),  # 320 s later: too long a gap
        Fix(T0 + 560, north(1850), LON0 + 0.01),
    ]  # 670 m east of the road: ignored
    got = passages(Route.of(stops), fixes)
    assert set(got) == {2}  # stop 2 (500 m) between the first fixes; 3, 4 across the gap; 1 never


def test_a_route_that_comes_back_on_itself_is_followed_in_order() -> None:
    # Out 1 km north and back: stops 1..3 going, 4..5 returning past the same street.
    out = [
        StopTime(1, north(0), LON0, T0),
        StopTime(2, north(500), LON0, T0 + 120),
        StopTime(3, north(1000), LON0, T0 + 240),
    ]
    back = [
        StopTime(4, north(500), LON0 + 0.0003, T0 + 360),
        StopTime(5, north(0), LON0 + 0.0003, T0 + 480),
    ]
    fixes = [
        Fix(
            T0 + 60 * k,
            north(250 * k if k <= 4 else 250 * (8 - k)),
            LON0 + (0 if k <= 4 else 0.0003),
        )
        for k in range(9)
    ]
    got = passages(Route.of(out + back), fixes)
    assert got[2] == pytest.approx(T0 + 120, abs=5)  # first time past 500 m
    assert got[4] == pytest.approx(T0 + 360, abs=5)  # second time, on the way back


def test_the_board_carries_the_last_delay_forward() -> None:
    stops = line()
    snap = Snapshot(T0, (Update(2, 60, None), Update(4, None, T0 + 500)))
    assert predicted(snap, stops[1]) == stops[1].scheduled + 60
    assert predicted(snap, stops[2]) == stops[2].scheduled + 60  # carried forward from stop 2
    assert predicted(snap, stops[3]) == T0 + 500  # its own absolute time
    assert predicted(snap, stops[0]) is None  # only later updates: they say nothing about it
    assert predicted(Snapshot(T0, (Update(3, None, None, skipped=True),)), stops[2]) is None


def test_errors_by_lead_with_the_timetable_alongside() -> None:
    stops = line()
    late = 180  # the bus runs 3 minutes late all the way
    fixes = drive(500 / 120, T0 - 120, T0 + 1200, step=60, late=late + 120)
    board = Snapshot(T0, (Update(1, 120, None),))  # the board says 2 minutes late
    errors, timed, total = trip_errors(stops, [board], fixes)
    assert (timed, total) == (5, 5)  # every stop but the first (a bus waits there)
    by_stop = sorted(errors, key=lambda e: e.lead_s)
    assert [e.bucket for e in by_stop] == ["2-5", "5-10", "5-10", "10-15", "10-15"]
    assert all(e.error_s == pytest.approx(60, abs=2) for e in errors)  # a minute later than shown
    assert all(e.timetable_s == pytest.approx(180, abs=2) for e in errors)  # 3 minutes vs timetable
    assert lead_bucket(-1) is None and lead_bucket(30 * 60) is None


def test_stops_already_passed_are_not_scored() -> None:
    stops = line()
    fixes = drive(500 / 120, T0, T0 + 900, step=60)
    late_snapshot = Snapshot(T0 + 300, (Update(1, 0, None),))
    errors, _, _ = trip_errors(stops, [late_snapshot], fixes)
    assert len(errors) == 3  # stops 4, 5, 6 still ahead at T0+300 (stop 3 passed at T0+240)


def test_accumulators_merge_and_count_trips_not_predictions() -> None:
    stops = line()
    fixes = drive(500 / 120, T0 - 120, T0 + 1200, step=60, late=240)
    errors, _, _ = trip_errors(stops, [Snapshot(T0, (Update(1, 120, None),))], fixes)
    one = accumulate(errors, "Dublin Bus")
    two = {k: v.merge(one[k]) for k, v in one.items()}
    for k, acc in two.items():
        assert acc.trips == 2 and acc.n == 2 * one[k].n
        assert sum(acc.hist) == acc.n and len(acc.hist) == HIST_BINS
        assert Acc.from_row(acc.to_row()) == acc
    s = summary(two[("Dublin Bus", "5-10")])
    assert s["median_min"] == pytest.approx(0.25, abs=0.5)
    # 2 trips: the interval on "within 2 minutes" is wide, as it should be
    assert s["within_2_min"]["hi"] - s["within_2_min"]["lo"] > 0.5


def test_quantiles_from_the_histogram() -> None:
    a = Acc()
    for e in range(-120, 121, 60):  # -2, -1, 0, 1, 2 minutes
        a.hist[(e + 600) // 30] += 1
    assert quantile(a.hist, 0.5) == pytest.approx(0.25)  # middle of the 0..30 s bin
    assert quantile([0] * HIST_BINS, 0.5) is None


def test_document_matches_its_schema() -> None:
    stops = line()
    fixes = drive(500 / 120, T0 - 120, T0 + 1200, step=60, late=240)
    errors, timed, total = trip_errors(stops, [Snapshot(T0, (Update(1, 120, None),))], fixes)
    doc = accuracy_doc(
        accumulate(errors, "Dublin Bus"), ["2026-11-18"], (timed, total), "2026-11-19T05:00:00Z"
    )
    jsonschema.validate(doc, schemas.load(schemas.EXPORT["accuracy.json"]))
    assert [r["lead_min"] for r in doc["by_lead"]] == [
        "0-2",
        "2-5",
        "5-10",
        "10-15",
        "15-20",
        "20-30",
    ]
    assert doc["by_operator"][0]["agency"] == "Dublin Bus"


@given(st.floats(1.0, 15.0), st.integers(30, 150), st.integers(-300, 900))
def test_passages_are_within_the_gap_whatever_the_speed(speed: float, step: int, late: int) -> None:
    stops = line()
    fixes = drive(speed, T0 - 600, T0 + 3600, step=step, late=late)
    got = passages(Route.of(stops), fixes)
    assert 1 not in got  # the first stop: departure, not a passage
    for s in stops:
        if s.seq in got:
            truth = T0 - 600 + late + (s.seq - 1) * 500 / speed
            assert abs(got[s.seq] - truth) <= step  # never off by more than the sampling gap


def test_a_bus_waiting_at_a_stop_is_timed_on_arrival_not_departure() -> None:
    # A diagonal route, where a fix exactly at a stop and the stop's own distance along the route
    # can differ in the last float digit (found by the integration test: the stop was timed when
    # the bus left, 18 minutes late).
    a, b, c = (53.3498, -6.2603), (53.3394, -6.2490), (53.3220, -6.2372)
    stops = [StopTime(1, *a, T0), StopTime(2, *b, T0 + 1200), StopTime(3, *c, T0 + 2400)]
    fixes = [Fix(T0 + 40 + 120 * k, *(a if k < 8 else b if k < 17 else c)) for k in range(22)]
    got = passages(Route.of(stops), fixes)
    assert got == {2: T0 + 40 + 120 * 8, 3: T0 + 40 + 120 * 17}
