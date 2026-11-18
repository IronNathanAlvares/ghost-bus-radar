"""JSON Schemas for every Kafka payload and for the public export. Tests validate real output
against them."""

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
#: The public export contract (data/v1): document path pattern -> schema file
EXPORT = {
    "days/": "export_day.v1.json",
    "league/": "export_league.v1.json",
    "routes/": "export_route.v1.json",
    "stops.json": "export_stops.v1.json",
    "meta.json": "export_meta.v1.json",
    "calibration.json": "export_calibration.v1.json",
}
#: The "will my bus come?" API (ADR-0012)
DEPARTURES = "api_departures.v1.json"
STOP_INDEX_POINTER = "api_stop_index_pointer.v1.json"


def load(filename: str) -> dict[str, object]:
    text = resources.files(__name__).joinpath(filename).read_text(encoding="utf-8")
    schema: dict[str, object] = json.loads(text)
    return schema
