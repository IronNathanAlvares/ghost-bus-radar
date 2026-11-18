from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from ghostbus_core.outcomes import (
    DEFAULT,
    RATED,
    UNTRACKED,
    Evidence,
    Outcome,
    Params,
    classify,
    close_time,
    in_rates,
    wilson,
)

START = datetime(2026, 11, 15, 20, 0, tzinfo=UTC)
BASE = Evidence(
    route_type="3",
    scheduled_start=START,
    cancelled_at=None,
    tu_snapshots=12,
    tu_vehicle=True,
    vp_positions=15,
    progress=0.95,
    tu_gap=False,
    vp_gap=False,
)


def ev(**kw: object) -> Evidence:
    return replace(BASE, **kw)  # type: ignore[arg-type]


def outcome(**kw: object) -> Outcome:
    return classify(ev(**kw))[0]


def test_ran() -> None:
    assert outcome() is Outcome.RAN
    assert outcome(progress=0.6) is Outcome.RAN


def test_out_of_scope_for_rail_and_luas() -> None:
    assert outcome(route_type="2", vp_positions=0, progress=None) is Outcome.OUT_OF_SCOPE
    assert outcome(route_type="0", vp_positions=0, progress=None) is Outcome.OUT_OF_SCOPE


def test_cancellations() -> None:
    early = START - timedelta(minutes=40)
    late = START - timedelta(minutes=5)
    assert outcome(cancelled_at=early, vp_positions=0, progress=None) is Outcome.CANCELLED_EARLY
    assert outcome(cancelled_at=late, vp_positions=0, progress=None) is Outcome.CANCELLED_LATE
    after = START + timedelta(minutes=10)
    assert outcome(cancelled_at=after, vp_positions=0, progress=None) is Outcome.CANCELLED_LATE
    exactly = START - timedelta(minutes=15)
    assert outcome(cancelled_at=exactly, vp_positions=0, progress=None) is Outcome.CANCELLED_EARLY


def test_late_cancellation_needs_coverage() -> None:
    late = START - timedelta(minutes=5)
    o, reason = classify(ev(cancelled_at=late, tu_gap=True, vp_positions=0, progress=None))
    assert o is Outcome.NO_DATA and "gap" in reason


def test_early_cancellation_stands_despite_a_trip_updates_gap() -> None:
    early = START - timedelta(minutes=40)
    assert (
        outcome(cancelled_at=early, tu_gap=True, vp_positions=0, progress=None)
        is Outcome.CANCELLED_EARLY
    )


def test_cancellation_with_a_vehicle_gap_is_no_data() -> None:
    """Found by the outage demo: the gap could hide that the cancelled trip ran anyway."""
    early = START - timedelta(minutes=40)
    o, reason = classify(ev(cancelled_at=early, vp_gap=True, progress=0.2))
    assert o is Outcome.NO_DATA and "ran anyway" in reason


def test_ran_despite_cancellation() -> None:
    assert outcome(cancelled_at=START - timedelta(minutes=60)) is Outcome.RAN


def test_vanished_and_untracked() -> None:
    assert outcome(progress=0.3) is Outcome.VANISHED
    # Reporting, but never within range of a stop (a bus parked at the depot): not "vanished".
    assert outcome(progress=0.0) is Outcome.PHANTOM_LIVE
    assert outcome(progress=0.0, tu_vehicle=False) is Outcome.PHANTOM_LIVE
    assert outcome(vp_positions=0, progress=None) is Outcome.PHANTOM_LIVE
    assert outcome(vp_positions=0, progress=None, tu_vehicle=False) is Outcome.GHOST
    assert outcome(vp_positions=0, progress=None, tu_vehicle=False, tu_snapshots=0) is Outcome.GHOST


def test_gaps_turn_absence_into_no_data() -> None:
    assert outcome(progress=0.3, vp_gap=True) is Outcome.NO_DATA
    assert outcome(progress=0.3, tu_gap=True) is Outcome.NO_DATA  # found by Hypothesis
    assert outcome(vp_positions=0, progress=None, vp_gap=True) is Outcome.NO_DATA
    assert outcome(vp_positions=0, progress=None, tu_gap=True) is Outcome.NO_DATA
    assert outcome(vp_gap=True, tu_gap=True) is Outcome.RAN  # positive evidence stands


evidence = st.builds(
    Evidence,
    route_type=st.sampled_from(["0", "2", "3"]),
    scheduled_start=st.just(START),
    cancelled_at=st.one_of(
        st.none(), st.integers(-180, 60).map(lambda m: START + timedelta(minutes=m))
    ),
    tu_snapshots=st.integers(0, 50),
    tu_vehicle=st.booleans(),
    vp_positions=st.integers(0, 60),
    progress=st.one_of(st.none(), st.floats(0, 1)),
    tu_gap=st.booleans(),
    vp_gap=st.booleans(),
)


@settings(max_examples=500)
@given(evidence)
def test_a_collection_gap_never_creates_an_untracked_outcome(e: Evidence) -> None:
    """The honesty rule: our own downtime can never make a trip look like a ghost."""
    o, _ = classify(e)
    if e.tu_gap or e.vp_gap:
        assert o not in UNTRACKED and o is not Outcome.VANISHED


@settings(max_examples=500)
@given(evidence)
def test_closing_a_gap_can_only_make_the_outcome_more_specific(e: Evidence) -> None:
    """With full coverage, NO_DATA resolves to a real outcome; other outcomes do not change."""
    with_gap, _ = classify(e)
    without_gap, _ = classify(replace(e, tu_gap=False, vp_gap=False))
    assert without_gap is not Outcome.NO_DATA
    if with_gap is not Outcome.NO_DATA:
        assert with_gap is without_gap


@settings(max_examples=500)
@given(evidence, st.floats(0.05, 0.95), st.floats(0.05, 0.95))
def test_raising_the_ran_threshold_never_creates_runs(e: Evidence, a: float, b: float) -> None:
    lo, hi = sorted((a, b))
    if classify(e, replace(DEFAULT, ran_progress=hi))[0] is Outcome.RAN:
        assert classify(e, replace(DEFAULT, ran_progress=lo))[0] is Outcome.RAN


@settings(max_examples=500)
@given(evidence)
def test_every_trip_in_rates_gets_a_rated_outcome(e: Evidence) -> None:
    """The rate sample is chosen by coverage, not by outcome, and none of it is NO_DATA."""
    if in_rates(e):
        assert classify(e)[0] in RATED


def test_a_run_through_a_gap_is_ran_but_not_in_rates() -> None:
    """Found on 15 November: 1,039 of 1,527 RAN trips were decided through a collection gap,
    where a trip that did not run would have been NO_DATA. Counting them inflated the ran rate
    from 85% to 95%."""
    e = replace(BASE, vp_gap=True)
    assert classify(e)[0] is Outcome.RAN
    assert not in_rates(e)
    assert in_rates(BASE)
    assert not in_rates(replace(BASE, route_type="2"))


def test_rated_excludes_no_data_and_out_of_scope() -> None:
    assert Outcome.NO_DATA not in RATED and Outcome.OUT_OF_SCOPE not in RATED
    assert UNTRACKED <= RATED


def test_params_version_changes_with_any_threshold() -> None:
    assert DEFAULT.version != Params(ran_progress=0.5).version
    assert DEFAULT.version != Params(cancel_lead_min=10).version
    assert DEFAULT.version.startswith("rules")


def test_close_time() -> None:
    assert close_time(START) == START + timedelta(hours=1)


def test_wilson() -> None:
    lo, hi = wilson(5, 100)
    assert lo == pytest.approx(0.0215, abs=1e-3) and hi == pytest.approx(0.1118, abs=1e-3)
    assert wilson(0, 0) == (0.0, 1.0)
    lo0, hi0 = wilson(0, 30)
    assert lo0 == 0.0 and 0.10 < hi0 < 0.12
