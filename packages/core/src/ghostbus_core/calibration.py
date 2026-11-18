"""How good is "will my bus come?": scoring backtest predictions (roadmap 2.4, ADR-0014). Pure.

A prediction is what the API would have said about one trip at one lead time before its scheduled
start, with the live status of that moment, and whether the trip then ran. This module turns a
list of them into the published scores:

- **Brier score**: mean of (p - y)^2. 0 is perfect; always saying 0.5 scores 0.25.
- **Baselines**: "always comes" (p = 1, what a timetable implies) and the base rate (p = the share
  of trips that ran in the history window, the same for every trip).
- **Brier skill score**: 1 - model / baseline. Above 0 means the model beats the baseline.
- **Reliability**: ten bins of predicted p; mean predicted vs share that ran, with a 95% interval.
- **Expected calibration error**: the count-weighted mean gap between the two.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any

from ghostbus_core.departures import FRESH_S, MAX_LIVE_AGE_S
from ghostbus_core.export import ATTRIBUTION
from ghostbus_core.outcomes import wilson

SCHEMA_VERSION = "v1"
#: Minutes before the scheduled start at which the model is asked.
LEADS_MIN = (30, 15, 5, 1)
BINS = 10
WINDOW_DAYS = 28
DIGITS = 4


@dataclass(frozen=True)
class Prediction:
    service_date: date
    lead_min: int
    status: str
    p: float | None  # None: no history for this route yet (counted, not scored)
    y: int  # 1 if the trip ran
    base_rate: float  # the history window's share of trips that ran (the base-rate baseline)


def brier(pairs: Iterable[tuple[float, int]]) -> float | None:
    pairs = list(pairs)
    if not pairs:
        return None
    return sum((p - y) ** 2 for p, y in pairs) / len(pairs)


def _r(x: float | None) -> float | None:
    return None if x is None else round(x, DIGITS)


def _skill(model: float | None, baseline: float | None) -> float | None:
    if model is None or not baseline:
        return None
    return 1 - model / baseline


def summary(preds: Sequence[Prediction]) -> dict[str, Any]:
    """Scores for one group of predictions (only those with a probability are scored)."""
    scored = [x for x in preds if x.p is not None]
    b_model = brier((x.p, x.y) for x in scored if x.p is not None)
    b_always = brier((1.0, x.y) for x in scored)
    b_base = brier((x.base_rate, x.y) for x in scored)
    return {
        "predictions": len(preds),
        "scored": len(scored),
        "without_history": len(preds) - len(scored),
        "mean_predicted": _r(sum(x.p for x in scored if x.p is not None) / len(scored))
        if scored
        else None,
        "share_ran": _r(sum(x.y for x in scored) / len(scored)) if scored else None,
        "brier": _r(b_model),
        "brier_always_comes": _r(b_always),
        "brier_base_rate": _r(b_base),
        "skill_vs_always_comes": _r(_skill(b_model, b_always)),
        "skill_vs_base_rate": _r(_skill(b_model, b_base)),
        "calibration_error": _r(calibration_error(scored)),
    }


def bins(preds: Sequence[Prediction], n_bins: int = BINS) -> list[dict[str, Any]]:
    """Reliability diagram data: one row per non-empty bin of predicted probability."""
    groups: dict[int, list[Prediction]] = defaultdict(list)
    for x in preds:
        if x.p is not None:
            groups[min(int(x.p * n_bins), n_bins - 1)].append(x)
    out = []
    for b in sorted(groups):
        g = groups[b]
        k = sum(x.y for x in g)
        lo, hi = wilson(k, len(g))
        out.append(
            {
                "from": round(b / n_bins, 2),
                "to": round((b + 1) / n_bins, 2),
                "n": len(g),
                "mean_predicted": _r(sum(x.p for x in g if x.p is not None) / len(g)),
                "share_ran": _r(k / len(g)),
                "lo": _r(lo),
                "hi": _r(hi),
            }
        )
    return out


def calibration_error(preds: Sequence[Prediction], n_bins: int = BINS) -> float | None:
    rows = bins(preds, n_bins)
    total = sum(r["n"] for r in rows)
    if not total:
        return None
    gaps = sum(r["n"] * abs(float(r["mean_predicted"]) - float(r["share_ran"])) for r in rows)
    return float(gaps) / float(total)


def calibration_doc(
    preds: Sequence[Prediction],
    model: str,
    generated_at: str,
    window_days: int = WINDOW_DAYS,
) -> dict[str, Any]:
    """``data/v1/calibration.json``: overall, by lead time and by live status, plus reliability."""
    days = sorted({x.service_date for x in preds})
    by_lead: dict[int, list[Prediction]] = defaultdict(list)
    by_status: dict[str, list[Prediction]] = defaultdict(list)
    for x in preds:
        by_lead[x.lead_min].append(x)
        by_status[x.status].append(x)
    overall = summary(preds)
    return {
        "schema": f"ghostbus.calibration.{SCHEMA_VERSION}",
        "attribution": ATTRIBUTION,
        "model": model,
        "generated_at": generated_at,
        "window_days": window_days,
        "test_days": [d.isoformat() for d in days],
        "leads_min": list(LEADS_MIN),
        "overall": overall,
        "beats_always_comes": bool(
            overall["skill_vs_always_comes"] is not None and overall["skill_vs_always_comes"] > 0
        ),
        "beats_base_rate": bool(
            overall["skill_vs_base_rate"] is not None and overall["skill_vs_base_rate"] > 0
        ),
        "reliability": bins(preds),
        "by_lead": [
            {"lead_min": lead} | summary(by_lead[lead]) | {"reliability": bins(by_lead[lead])}
            for lead in sorted(by_lead, reverse=True)
        ],
        "by_status": [
            {"status": s} | summary(by_status[s])
            for s in sorted(by_status, key=lambda s: -len(by_status[s]))
        ],
    }


def live_status(
    snapshot_age_s: float | None, cancelled: bool, tracked: bool, assigned: bool
) -> tuple[str, bool]:
    """(status, fresh) for one trip at one moment: the API's rules (ADR-0012), at trip level.

    No snapshot within 15 minutes: ``no_live_data``. Otherwise cancelled beats tracked beats
    assigned beats scheduled; ``fresh`` is a snapshot within 5 minutes.
    """
    if snapshot_age_s is None or snapshot_age_s > MAX_LIVE_AGE_S:
        return "no_live_data", False
    fresh = snapshot_age_s <= FRESH_S
    if cancelled:
        return "cancelled", fresh
    if tracked:
        return "tracked", fresh
    if assigned:
        return "assigned", fresh
    return "scheduled", fresh
