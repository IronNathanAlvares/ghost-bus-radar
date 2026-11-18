from __future__ import annotations

from datetime import UTC, datetime, timedelta

from hypothesis import given
from hypothesis import strategies as st

from ghostbus_core.coverage import collection_gaps, overlaps

T = datetime(2026, 11, 15, 20, 0, tzinfo=UTC)
M = timedelta(minutes=1)


def test_regular_collection_has_no_gaps() -> None:
    times = [T + 2 * i * M for i in range(31)]
    assert collection_gaps(times, T, T + 60 * M, 5 * M) == []


def test_outage_is_a_gap() -> None:
    times = [T + 2 * i * M for i in range(10)] + [T + 40 * M + 2 * i * M for i in range(11)]
    assert collection_gaps(times, T, T + 60 * M, 5 * M) == [(T + 18 * M, T + 40 * M)]


def test_edges_are_gaps() -> None:
    times = [T + 20 * M, T + 22 * M]
    assert collection_gaps(times, T, T + 60 * M, 5 * M) == [
        (T, T + 20 * M),
        (T + 22 * M, T + 60 * M),
    ]
    assert collection_gaps([], T, T + 60 * M, 5 * M) == [(T, T + 60 * M)]


def test_overlaps() -> None:
    gaps = [(T + 18 * M, T + 40 * M)]
    assert overlaps(gaps, T + 30 * M, T + 50 * M)
    assert not overlaps(gaps, T + 40 * M, T + 50 * M)  # touching is not overlapping
    assert not overlaps(gaps, T, T + 18 * M)


@given(st.lists(st.integers(0, 600), max_size=80), st.integers(1, 30))
def test_every_instant_is_in_a_gap_or_near_a_snapshot(minutes: list[int], max_gap: int) -> None:
    times = [T + m * M for m in minutes]
    end = T + 600 * M
    gaps = collection_gaps(times, T, end, max_gap * M)
    for probe in range(0, 601, 7):
        p = T + probe * M
        in_gap = any(g0 <= p <= g1 for g0, g1 in gaps)
        near = any(abs(t - p) <= max_gap * M for t in times) and times
        assert in_gap or near
