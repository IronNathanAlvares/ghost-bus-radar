from __future__ import annotations

import pytest

from ghostbus_jobs.stream_metrics import flatten

PROGRESS = {
    "id": "8c1b",
    "runId": "f00d",
    "name": "silver_vehicle_positions",
    "timestamp": "2026-11-15T20:30:00.000Z",
    "batchId": 12,
    "numInputRows": 785,
    "durationMs": {"addBatch": 1450, "triggerExecution": 1702},
    "eventTime": {
        "avg": "2026-11-15T20:28:40.000Z",
        "max": "2026-11-15T20:29:51.000Z",
        "min": "2026-11-15T20:01:12.000Z",
        "watermark": "2026-11-15T20:19:30.000Z",
    },
    "stateOperators": [
        {
            "operatorName": "dedupeWithinWatermark",
            "numRowsTotal": 5210,
            "numRowsDroppedByWatermark": 37,
            "memoryUsedBytes": 1048576,
        }
    ],
    "sink": {"description": "IcebergSink", "numOutputRows": 702},
}


def test_flatten() -> None:
    r = flatten(PROGRESS)
    assert r["query"] == "silver_vehicle_positions" and r["batch_id"] == 12
    assert r["input_rows"] == 785 and r["output_rows"] == 702
    assert r["batch_ms"] == 1702 and r["add_batch_ms"] == 1450
    assert r["late_rows"] == 37 and r["state_rows"] == 5210
    assert r["watermark_lag_s"] == pytest.approx(630.0)  # 20:30:00 - 20:19:30


def test_flatten_before_any_watermark() -> None:
    """Spark reports the epoch as the watermark until it has seen event times."""
    r = flatten(
        {
            "name": "bronze",
            "timestamp": "2026-11-15T20:30:00.000Z",
            "numInputRows": 0,
            "eventTime": {"watermark": "1970-01-01T00:00:00.000Z"},
        }
    )
    assert r["watermark_lag_s"] is None and r["late_rows"] == 0 and r["output_rows"] is None
