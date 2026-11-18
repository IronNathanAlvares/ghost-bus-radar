"""The call schedule must never break the NTA limit of one call per 60 s per token."""

from __future__ import annotations

from collections import Counter

import pytest
from hypothesis import given
from hypothesis import strategies as st

from ghostbus_core.feeds import ALERTS_EVERY, MIN_CALL_SPACING_S, Feed, feed_for_slot, minute_slot
from ghostbus_core.pacing import (
    MAX_SKIP_SLOTS,
    REALIGN_TOLERANCE_S,
    FetchStatus,
    backoff_slots,
    next_call_at,
    planned_call_at,
)

NOW = 1_794_770_401.0


def test_feed_urls_and_topics() -> None:
    assert Feed.TRIP_UPDATES.url == "https://api.nationaltransport.ie/gtfsr/v2/TripUpdates"
    assert Feed.VEHICLES.url.endswith("/Vehicles")
    assert Feed.ALERTS.url.endswith("/alerts")
    assert Feed.VEHICLES.topic.name == "gtfsr.vehicle_positions.v1"


def test_schedule_shares_one_call_per_minute() -> None:
    hour = Counter(feed_for_slot(s) for s in range(1000 * 60, 1000 * 60 + 60))
    assert hour[Feed.ALERTS] == 60 // ALERTS_EVERY
    assert hour[Feed.TRIP_UPDATES] == hour[Feed.VEHICLES] == 28


def test_alert_slot_shifts_the_alternation() -> None:
    assert [feed_for_slot(s) for s in range(12, 17)] == [
        Feed.TRIP_UPDATES,
        Feed.VEHICLES,
        Feed.ALERTS,
        Feed.TRIP_UPDATES,
        Feed.VEHICLES,
    ]


@given(st.integers(min_value=0, max_value=10**8))
def test_every_feed_is_called_within_any_three_minutes(start: int) -> None:
    window = [feed_for_slot(s) for s in range(start, start + 3)]
    assert Feed.TRIP_UPDATES in window and Feed.VEHICLES in window


def test_minute_slot() -> None:
    assert minute_slot(59.9) == 0
    assert minute_slot(60.0) == 1


@given(
    now=st.floats(min_value=1e9, max_value=2e9),
    offset=st.floats(min_value=0, max_value=59.99),
    since_last=st.one_of(st.none(), st.floats(min_value=0, max_value=600)),
    skip=st.integers(min_value=0, max_value=MAX_SKIP_SLOTS),
)
def test_next_call_is_in_the_future_and_never_within_60s(
    now: float, offset: float, since_last: float | None, skip: int
) -> None:
    last = None if since_last is None else now - since_last
    at = next_call_at(now, offset, last, skip)
    assert at > now
    if last is not None:
        assert at - last >= MIN_CALL_SPACING_S
    assert at - now <= 60 * (skip + 1) + MIN_CALL_SPACING_S + 1


def test_next_call_lands_on_the_offset() -> None:
    minute = 29_912_840 * 60.0
    assert next_call_at(minute + 3, offset_s=10, last_call_s=None) == minute + 10
    assert next_call_at(minute + 11, offset_s=10, last_call_s=None) == minute + 70
    assert next_call_at(minute + 11, offset_s=10, last_call_s=None, skip_slots=2) == minute + 190


def test_slow_call_pushes_the_next_one_out() -> None:
    minute = 29_912_840 * 60.0
    # last call started at :12 (two seconds late); the :10 slot of the next minute is too soon
    at = next_call_at(minute + 30, offset_s=10, last_call_s=minute + 12)
    assert at == pytest.approx(minute + 72)


@pytest.mark.parametrize("bad", [-1.0, 60.0])
def test_offset_out_of_range(bad: float) -> None:
    with pytest.raises(ValueError):
        next_call_at(NOW, bad, None)


@pytest.mark.parametrize(
    ("code", "status"),
    [
        (200, FetchStatus.OK),
        (429, FetchStatus.THROTTLED),
        (401, FetchStatus.CLIENT_ERROR),
        (404, FetchStatus.CLIENT_ERROR),
        (500, FetchStatus.SERVER_ERROR),
        (503, FetchStatus.SERVER_ERROR),
    ],
)
def test_status_from_http(code: int, status: FetchStatus) -> None:
    assert FetchStatus.from_http(code) is status


def test_backoff() -> None:
    assert backoff_slots(FetchStatus.OK, 0) == 0
    assert [backoff_slots(FetchStatus.SERVER_ERROR, n) for n in range(1, 7)] == [0, 1, 3, 7, 15, 15]
    assert backoff_slots(FetchStatus.THROTTLED, 1) == 1
    assert backoff_slots(FetchStatus.THROTTLED, 1, retry_after_s=300) == 4
    assert backoff_slots(FetchStatus.THROTTLED, 1, retry_after_s=99_999) == MAX_SKIP_SLOTS


@given(
    st.floats(min_value=1e9, max_value=2e9),
    st.floats(min_value=0, max_value=59.9),
    st.floats(min_value=0, max_value=3600),
    st.integers(min_value=0, max_value=15),
)
def test_stateless_plan_never_breaks_spacing_or_runs_in_the_past(
    last: float, offset: float, since: float, skip: int
) -> None:
    now = last + since
    at = planned_call_at(now, offset, last, skip)
    assert at >= last + MIN_CALL_SPACING_S
    assert at > now


def test_stateless_plan_matches_the_long_running_poller() -> None:
    """Right after a call at 12:00:15, both plan 12:01:15; after a 3-slot backoff, 12:04:15."""
    last = 1_794_830_415.0  # a :15 point
    assert planned_call_at(last + 50, 15, last, 0) == next_call_at(last, 15, last, 0) == last + 60
    assert planned_call_at(last + 50, 15, last, 3) == last + 240
    # The 12:01 invocation never ran; at 12:02:05 the next call is 12:02:15.
    assert planned_call_at(last + 110, 15, last, 0) == last + 120


def test_stateless_plan_without_history() -> None:
    assert planned_call_at(1_794_830_402.0, 15, None) == 1_794_830_415.0


def test_a_new_offset_takes_effect_after_one_longer_gap() -> None:
    """Found on 18 Nov: calls at :15 stayed at :15 after the offset moved to :25."""
    minute = 29_912_840 * 60.0
    at = next_call_at(minute + 16, offset_s=25, last_call_s=minute + 15)
    assert at == minute + 85  # 70 s gap, then on :25 from now on
    assert next_call_at(at + 1, offset_s=25, last_call_s=at) == at + 60


def test_far_off_the_offset_gives_up_one_minute_to_realign() -> None:
    minute = 29_912_840 * 60.0
    # last call at :40 (a handover from a poller on another second); offset :15
    at = next_call_at(minute + 41, offset_s=15, last_call_s=minute + 40)
    assert at == minute + 135  # minute+1 :15 is too soon, minute+1 :40 is off by 25 s
    assert at - (minute + 40) >= MIN_CALL_SPACING_S


@given(
    start=st.floats(min_value=1e9, max_value=2e9),
    offset=st.floats(min_value=0, max_value=59.99),
    jitter=st.lists(st.floats(min_value=0, max_value=2), min_size=30, max_size=30),
)
def test_a_running_poller_settles_on_its_offset_and_keeps_60s(
    start: float, offset: float, jitter: list[float]
) -> None:
    """Simulate a loop whose wake-ups are up to 2 s late; spacing always holds, and after the
    every call lands within the tolerance (plus jitter) of the offset."""
    last: float | None = None
    now = start
    calls = []
    for j in jitter:
        at = next_call_at(now, offset, last)
        now = at + j  # the call actually starts a little late
        if last is not None:
            assert now - last >= MIN_CALL_SPACING_S
        calls.append(now)
        last = now
        now += 1.0
    # Late starts accumulate only up to the tolerance; then the plan realigns to the offset.
    bound = REALIGN_TOLERANCE_S + 2 + 1e-6
    drift = [(c - offset) % 60 for c in calls]
    assert all(d <= bound or d >= 60 - 1e-6 for d in drift), drift
