"""The quality gates catch every failure this project has actually had (replayed with the real
numbers), and stay quiet on a healthy day."""

from __future__ import annotations

from dataclasses import replace
from datetime import date

from ghostbus_core.quality import FAIL, OK, WARN, DayStats, evaluate, overall, render

HEALTHY = DayStats(
    service_date=date(2026, 11, 19),
    scheduled=19_767,
    scheduled_week_away=19_767,
    decided=19_767,
    bus_trips=17_000,
    no_data=150,
    in_rates=16_800,
    ran=15_000,
    untracked=1_300,
    vanished=300,
    cancelled_early=150,
    cancelled_late=50,
    minutes_expected=1_440,
    minutes_collected=1_440,
    rt_with_id=3_000_000,
    rt_unmatched=0,
    trailing_in_rates=100_000,
    trailing_untracked=7_900,
    worst_routes=[("Bus Éireann 101", 60, 14)],
    batch_cost_usd=0.041,
)


def status(s: DayStats, name: str) -> str:
    return next(c.status for c in evaluate(s) if c.name == name)


def test_a_healthy_day_is_ok() -> None:
    checks = evaluate(HEALTHY)
    assert overall(checks) == OK, [c for c in checks if c.status != OK]


def test_the_7003_trip_sunday_fails() -> None:
    """15 Nov 2026: two zips with one label left 7,003 of 12,185 trips; next Sunday had 12,184."""
    bad = replace(HEALTHY, scheduled=7_003, scheduled_week_away=12_184, decided=7_003)
    assert status(bad, "scheduled_vs_week") == FAIL
    # Every scheduled trip was decided, so a weaker "decided vs scheduled" check misses it.
    assert status(bad, "decided_share") == OK


def test_ids_lagging_a_publish_fail() -> None:
    """15-16 Nov 2026 before rules 5: 15,709 of 1,633,371 trip updates unmatched."""
    bad = replace(HEALTHY, rt_with_id=1_633_371, rt_unmatched=15_709)
    assert status(bad, "unmatched_ids") == FAIL


def test_a_sleeping_laptop_fails_completeness() -> None:
    """16 Nov 2026: the laptop poller made 471 of 1,440 calls."""
    assert status(replace(HEALTHY, minutes_collected=471), "completeness") == FAIL
    assert status(replace(HEALTHY, minutes_collected=1_420), "completeness") == WARN  # 98.6%


def test_reconcile_stopping_early_fails() -> None:
    assert status(replace(HEALTHY, decided=12_651), "decided_share") == FAIL


def test_a_bank_holiday_warns_but_does_not_fail() -> None:
    # A Saturday-sized timetable on a weekday.
    holiday = replace(HEALTHY, scheduled=16_147, decided=16_147)
    assert status(holiday, "scheduled_vs_week") == WARN


def test_untracked_shift_needs_both_size_and_significance() -> None:
    # 12% against 7.9%, but with n = 50 the interval (5.6-23.8%) includes 7.9%.
    small_sample = replace(HEALTHY, in_rates=50, untracked=6, ran=42)
    assert status(small_sample, "untracked_shift") == OK
    big_jump = replace(HEALTHY, untracked=2_500)  # 14.9% vs 7.9%, n = 16,800
    assert status(big_jump, "untracked_shift") == WARN


def test_the_email_says_fail_in_the_subject_and_why() -> None:
    bad = replace(HEALTHY, scheduled=7_003, scheduled_week_away=12_184, decided=7_003)
    subject, body = render(bad, evaluate(bad))
    assert subject.startswith("Ghost Bus Radar Thu 19 Nov: FAIL")
    assert "[FAIL] scheduled_vs_week: 7,003 scheduled vs 12,184" in body
    assert "do not publish" in body
    subject, body = render(HEALTHY, evaluate(HEALTHY))
    assert subject == "Ghost Bus Radar Thu 19 Nov: OK, ran 89.3%"
    assert "Bus Éireann 101: 14 / 60" in body and "$0.041" in body
