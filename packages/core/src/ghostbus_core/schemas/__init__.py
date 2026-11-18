"""JSON Schemas for every Kafka payload. Tests validate real normaliser output against them."""

from __future__ import annotations

import json
from importlib import resources

#: topic name -> schema file
BY_TOPIC = {
    "gtfsr.trip_updates.v1": "trip_update.v1.json",
    "gtfsr.vehicle_positions.v1": "vehicle_position.v1.json",
    "gtfsr.alerts.v1": "alert.v1.json",
    "ingest.heartbeat.v1": "heartbeat.v1.json",
}
DLQ = "dlq.v1.json"


def load(filename: str) -> dict[str, object]:
    text = resources.files(__name__).joinpath(filename).read_text(encoding="utf-8")
    schema: dict[str, object] = json.loads(text)
    return schema
