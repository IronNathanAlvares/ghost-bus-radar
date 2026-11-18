"""Needs the local stack. Run inside the spark container:

docker compose -f local/docker-compose.yml run --rm spark pytest -m integration
"""

import math

import pytest

from ghostbus_jobs.smoke import run

pytestmark = pytest.mark.integration


def test_events_flow_from_kafka_to_iceberg() -> None:
    result = run(n_events=5, interval_s=0.5, trigger_ms=500, timeout_s=120)
    assert result.events_landed == result.events_sent
    assert not math.isnan(result.latency_ms_p95)
    # Generous bound: this proves the path works, the measurement lives in docs/measurements.
    assert result.latency_ms_p95 < 30_000
