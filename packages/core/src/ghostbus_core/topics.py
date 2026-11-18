"""Kafka topic catalogue. The single source of truth for names, partitions and retention.

Topic names carry a schema major version (``.v1``). A breaking payload change means a new topic,
not an in-place change, so old consumers and replays keep working.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

_DAY_MS = 24 * 60 * 60 * 1000
_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*\.v\d+(\.dlq)?$")


@dataclass(frozen=True)
class Topic:
    name: str
    partitions: int
    retention_ms: int
    description: str

    def __post_init__(self) -> None:
        if not _NAME_RE.match(self.name):
            raise ValueError(f"topic name {self.name!r} does not match {_NAME_RE.pattern}")
        if self.partitions < 1:
            raise ValueError("partitions must be >= 1")

    def dlq(self) -> Topic:
        """Dead-letter topic for messages that failed to parse, kept longer for debugging."""
        return Topic(
            name=f"{self.name}.dlq",
            partitions=1,
            retention_ms=30 * _DAY_MS,
            description=f"Unparseable messages from {self.name}",
        )


TRIP_UPDATES = Topic(
    name="gtfsr.trip_updates.v1",
    partitions=6,
    retention_ms=7 * _DAY_MS,
    description="One message per GTFS-R TripUpdate entity, keyed by natural trip key",
)
VEHICLE_POSITIONS = Topic(
    name="gtfsr.vehicle_positions.v1",
    partitions=6,
    retention_ms=7 * _DAY_MS,
    description="One message per GTFS-R VehiclePosition entity, keyed by natural trip key",
)
ALERTS = Topic(
    name="gtfsr.alerts.v1",
    partitions=1,
    retention_ms=30 * _DAY_MS,
    description="One message per GTFS-R Alert entity",
)
HEARTBEAT = Topic(
    name="ingest.heartbeat.v1",
    partitions=1,
    retention_ms=30 * _DAY_MS,
    description="One record per poll: status, latency, entity count. Drives NO_DATA outcomes",
)
SMOKE = Topic(
    name="smoke.events.v1",
    partitions=1,
    retention_ms=1 * _DAY_MS,
    description="End-to-end smoke test and latency probe",
)

_PRIMARY: tuple[Topic, ...] = (TRIP_UPDATES, VEHICLE_POSITIONS, ALERTS, HEARTBEAT, SMOKE)
_WITH_DLQ: frozenset[str] = frozenset({TRIP_UPDATES.name, VEHICLE_POSITIONS.name, ALERTS.name})


def all_topics() -> tuple[Topic, ...]:
    """Every topic the platform needs, including dead-letter topics."""
    dlqs = tuple(t.dlq() for t in _PRIMARY if t.name in _WITH_DLQ)
    return _PRIMARY + dlqs
