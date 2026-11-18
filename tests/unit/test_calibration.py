"""Scoring the backtest (ADR-0014): Brier scores against known values, both baselines, reliability
bins, the live-status rules, and the published document's schema."""

from __future__ import annotations

from datetime import date

import jsonschema
import pytest
from hypothesis import given
from hypothesis import strategies as st

from ghostbus_core import schemas
from ghostbus_core.calibration import (
    Prediction,
    bins,
    brier,
    calibration_doc,
    calibration_error,
    live_status,
    summary,
)

D = date(2026, 11, 17)


def pred(
    p: float | None, y: int, lead: int = 5, status: str = "tracked", base: float = 0.8
) -> Prediction:
    return Prediction(D, lead, status, p, y, base)


def test_brier_known_values() -> None:
    assert brier([(1.0, 1), (0.0, 0)]) == 0.0
    assert brier([(0.5, 1), (0.5, 0)]) == 0.25
    assert brier([(0.9, 1), (0.9, 0)]) == pytest.approx((0.01 + 0.81) / 2)
    assert brier([]) is None


def test_baselines_and_skill() -> None:
    # 8 of 10 ran. A model that says 0.95 for the 8 and 0.2 for the 2 beats both baselines.
    preds = [pred(0.95, 1) for _ in range(8)] + [pred(0.2, 0) for _ in range(2)]
    s = summary(preds)
    assert s["brier_always_comes"] == 0.2  # (1-0)^2 for 2 of 10
    assert s["brier_base_rate"] == pytest.approx(0.16)  # 0.8: 8 x 0.04 + 2 x 0.64, over 10
    assert s["brier"] == pytest.approx((8 * 0.0025 + 2 * 0.04) / 10)
    assert s["skill_vs_always_comes"] > 0 and s["skill_vs_base_rate"] > 0
    assert (s["share_ran"], s["scored"]) == (0.8, 10)


def test_predictions_without_history_are_counted_not_scored() -> None:
    s = summary([pred(None, 1), pred(None, 0), pred(0.9, 1)])
    assert (s["predictions"], s["scored"], s["without_history"]) == (3, 1, 2)
    assert s["brier"] == pytest.approx(0.01)


def test_reliability_bins_and_a_perfectly_calibrated_model() -> None:
    # 10 at 0.3 (3 ran) and 10 at 0.9 (9 ran): exactly calibrated.
    preds = [pred(0.3, int(i < 3)) for i in range(10)] + [pred(0.9, int(i < 9)) for i in range(10)]
    rows = bins(preds)
    assert [(r["from"], r["n"], r["share_ran"]) for r in rows] == [(0.3, 10, 0.3), (0.9, 10, 0.9)]
    assert calibration_error(preds) == pytest.approx(0.0, abs=1e-9)
    assert rows[0]["lo"] < 0.3 < rows[0]["hi"]
    assert bins([pred(1.0, 1)])[0]["from"] == 0.9  # p = 1 falls in the top bin, not an 11th


def test_live_status_follows_the_api_rules() -> None:
    assert live_status(None, False, False, False) == ("no_live_data", False)
    assert live_status(901, True, True, True) == ("no_live_data", False)
    assert live_status(600, False, False, False) == ("scheduled", False)  # stale but usable
    assert live_status(60, True, True, True) == ("cancelled", True)
    assert live_status(60, False, True, True) == ("tracked", True)
    assert live_status(60, False, False, True) == ("assigned", True)


def test_document_matches_its_schema() -> None:
    preds = [pred(0.95, 1, lead) for lead in (30, 15, 5, 1)] + [
        pred(0.6, 0, 30, "scheduled"),
        pred(None, 1, 5, "assigned"),
    ]
    doc = calibration_doc(preds, "heuristic-v1", "2026-11-18T05:00:00Z")
    jsonschema.validate(doc, schemas.load(schemas.EXPORT["calibration.json"]))
    assert [r["lead_min"] for r in doc["by_lead"]] == [30, 15, 5, 1]
    assert doc["test_days"] == ["2026-11-17"]
    assert doc["beats_always_comes"] is True


@given(
    st.lists(
        st.tuples(st.floats(0, 1), st.integers(0, 1), st.floats(0, 1)), min_size=1, max_size=200
    )
)
def test_scores_stay_in_range(rows: list[tuple[float, int, float]]) -> None:
    preds = [pred(p, y, base=b) for p, y, b in rows]
    s = summary(preds)
    for key in ("brier", "brier_always_comes", "brier_base_rate", "calibration_error"):
        assert s[key] is None or 0 <= s[key] <= 1
    for key in ("skill_vs_always_comes", "skill_vs_base_rate"):
        assert s[key] is None or s[key] <= 1
    assert sum(r["n"] for r in bins(preds)) == len(preds)


def test_murphy_decomposition_and_the_comparison_with_the_previous_model() -> None:
    # Current model: sharp and right. Previous: always 0.8 (no resolution).
    preds = [Prediction(D, 5, "tracked", 0.95, 1, 0.8, p_previous=0.8) for _ in range(8)] + [
        Prediction(D, 5, "scheduled", 0.1, 0, 0.8, p_previous=0.8) for _ in range(2)
    ]
    doc = calibration_doc(
        preds, "heuristic-v2", "2026-11-19T05:00:00Z", previous_model="heuristic-v1"
    )
    jsonschema.validate(doc, schemas.load(schemas.EXPORT["calibration.json"]))
    cur, prev = doc["comparison"]["current"], doc["comparison"]["previous"]
    assert cur["brier"] < prev["brier"]
    assert (
        prev["decomposition"]["resolution"] == 0.0
    )  # one forecast for everyone tells nobody apart
    assert cur["decomposition"]["resolution"] > 0
    assert cur["decomposition"]["uncertainty"] == pytest.approx(0.16)
    assert calibration_doc(preds, "m", "2026-11-19T05:00:00Z")["comparison"] is None
