"""Nightly data-quality gates and the daily summary (roadmap 1.1 and 1.2). Pure.

A wrong number published is worse than a late one. Each check below exists because of a real
failure in this project's history, and the thresholds are set so that failure would have been
caught (``tests/unit/test_quality.py`` replays them):

- ``scheduled_vs_week``: 15 Nov 2026 had 7,003 scheduled trips instead of 12,185 because two
  timetable zips shared a label. The following Sunday had 12,184: ratio 0.57.
- ``unmatched_ids``: 0.96% of realtime ids matched no trip after a timetable publish (fixed in
  rules version 5); the healthy rate is 0.00%.
- ``completeness``: the laptop poller collected about a third of the minutes while it slept.
- ``decided_share``: a reconcile that stops early leaves trips undecided.
- ``no_data_share``: gaps in collection shrink the measurable sample.
- ``untracked_shift``: a real change, or a code change, in the headline rate.

Statuses: ``ok``, ``warn`` (look at it), ``fail`` (do not publish this day's numbers).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from ghostbus_core.outcomes import wilson

OK, WARN, FAIL = "ok", "warn", "fail"
_RANK = {OK: 0, WARN: 1, FAIL: 2}


@dataclass(frozen=True)
class DayStats:
    service_date: date
    scheduled: int
    #: Scheduled trips on the same weekday one week away (before or after), if known.
    scheduled_week_away: int | None
    decided: int
    bus_trips: int
    no_data: int
    in_rates: int
    ran: int
    untracked: int
    vanished: int
    cancelled_early: int
    cancelled_late: int
    minutes_expected: int
    minutes_collected: int
    rt_with_id: int
    rt_unmatched: int
    #: Pooled over the 7 days before: trips in rates and untracked among them.
    trailing_in_rates: int = 0
    trailing_untracked: int = 0
    #: (route, trips in rates, untracked) with the most untracked trips.
    worst_routes: list[tuple[str, int, int]] = field(default_factory=list)
    batch_cost_usd: float | None = None


@dataclass(frozen=True)
class Check:
    name: str
    value: float | None
    status: str
    detail: str


def _band(value: float, warn_below: float, fail_below: float) -> str:
    return FAIL if value < fail_below else WARN if value < warn_below else OK


def _share(k: int, n: int) -> float | None:
    return k / n if n else None


def evaluate(s: DayStats) -> list[Check]:
    checks = []

    if s.scheduled_week_away:
        r = s.scheduled / s.scheduled_week_away
        off = abs(1 - r)
        status = FAIL if off > 0.25 else WARN if off > 0.10 else OK
        detail = (
            f"{s.scheduled:,} scheduled vs {s.scheduled_week_away:,} "
            "on the same weekday a week away"
        )
        if status != OK:
            detail += " (a bank holiday explains a warning; a fail means a timetable problem)"
        checks.append(Check("scheduled_vs_week", r, status, detail))
    else:
        checks.append(Check("scheduled_vs_week", None, WARN, "no same weekday loaded to compare"))

    v = _share(s.rt_unmatched, s.rt_with_id)
    if v is None:
        checks.append(Check("unmatched_ids", None, FAIL, "no realtime trips with an id"))
    else:
        # Healthy is 0.00%; the rules-5 bug was 0.96%, so fail well below that.
        status = FAIL if v > 0.005 else WARN if v > 0.0005 else OK
        checks.append(
            Check(
                "unmatched_ids", v, status, f"{s.rt_unmatched:,} of {s.rt_with_id:,} realtime ids"
            )
        )

    v = _share(s.minutes_collected, s.minutes_expected)
    assert v is not None
    checks.append(
        Check(
            "completeness",
            v,
            _band(v, 0.99, 0.90),
            f"{s.minutes_collected:,} of {s.minutes_expected:,} minutes collected",
        )
    )

    v = _share(s.decided, s.scheduled)
    if v is None:
        checks.append(Check("decided_share", None, FAIL, "no scheduled trips"))
    else:
        checks.append(
            Check(
                "decided_share",
                v,
                _band(v, 0.999, 0.99),
                f"{s.decided:,} of {s.scheduled:,} trips decided",
            )
        )

    v = _share(s.no_data, s.bus_trips)
    if v is not None:
        status = FAIL if v > 0.25 else WARN if v > 0.05 else OK
        checks.append(
            Check("no_data_share", v, status, f"{s.no_data:,} of {s.bus_trips:,} bus trips NO_DATA")
        )

    if s.in_rates and s.trailing_in_rates:
        today = s.untracked / s.in_rates
        before = s.trailing_untracked / s.trailing_in_rates
        lo, hi = wilson(s.untracked, s.in_rates)
        moved = abs(today - before) > 0.02 and not (lo <= before <= hi)
        checks.append(
            Check(
                "untracked_shift",
                today - before,
                WARN if moved else OK,
                f"untracked {100 * today:.1f}% (95% CI {100 * lo:.1f}-{100 * hi:.1f}) "
                f"vs {100 * before:.1f}% over the previous 7 days",
            )
        )
    return checks


def overall(checks: list[Check]) -> str:
    return max((c.status for c in checks), key=_RANK.__getitem__, default=OK)


def _rate(k: int, n: int) -> str:
    if not n:
        return "-"
    lo, hi = wilson(k, n)
    return f"{100 * k / n:.1f}% ({100 * lo:.1f}-{100 * hi:.1f})"


def render(s: DayStats, checks: list[Check]) -> tuple[str, str]:
    """Email subject and plain-text body for one service day."""
    status = overall(checks)
    mark = {OK: "OK", WARN: "WARN", FAIL: "FAIL"}[status]
    subject = f"Ghost Bus Radar {s.service_date:%a %d %b}: {mark}"
    if s.in_rates:
        subject += f", ran {100 * s.ran / s.in_rates:.1f}%"
    lines = [
        f"Service day {s.service_date:%A %d %B %Y}. Overall: {mark}.",
        "",
        f"Bus trips in rates (whole window collected): {s.in_rates:,}",
        f"  Ran              {_rate(s.ran, s.in_rates)}",
        f"  Untracked        {_rate(s.untracked, s.in_rates)}   (ghost + phantom)",
        f"  Vanished         {_rate(s.vanished, s.in_rates)}",
        f"  Cancelled early  {_rate(s.cancelled_early, s.in_rates)}",
        f"  Cancelled late   {_rate(s.cancelled_late, s.in_rates)}",
        "  (95% Wilson intervals in brackets)",
        "",
        "Checks:",
    ]
    for c in checks:
        lines.append(f"  [{c.status.upper():4}] {c.name}: {c.detail}")
    if s.worst_routes:
        lines += ["", "Most untracked trips (route: untracked / in rates):"]
        lines += [f"  {r}: {u} / {n}" for r, n, u in s.worst_routes]
    if s.batch_cost_usd is not None:
        lines += ["", f"Last night's batch cost: ${s.batch_cost_usd:.3f}"]
    if status == FAIL:
        lines += [
            "",
            "FAIL: do not publish this day's numbers until the failing check is understood.",
        ]
    lines += [
        "",
        'Athena: workgroup ghostbus, table "s3tablescatalog/ghostbus-lake".gold.data_quality',
    ]
    return subject, "\n".join(lines) + "\n"
