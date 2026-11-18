"""Spark schemas for the Kafka payloads. Twins of the JSON Schemas in ``ghostbus_core/schemas``.

An integration test parses real normaliser output with these and checks nothing is lost, so the
two cannot drift apart silently.
"""

from __future__ import annotations

_TRIP = (
    "STRUCT<trip_id: STRING, route_id: STRING, direction_id: INT, start_time: STRING, "
    "start_date: STRING, schedule_relationship: STRING>"
)
_VEHICLE = "STRUCT<id: STRING, label: STRING, license_plate: STRING>"
_EVENT = "STRUCT<delay: INT, time: BIGINT, uncertainty: INT>"
_ENVELOPE = (
    "schema_version INT, fetch_id STRING, fetched_at_ms BIGINT, feed_timestamp BIGINT, "
    "entity_id STRING, is_deleted BOOLEAN, trip_key STRING"
)

TRIP_UPDATE = (
    f"{_ENVELOPE}, trip {_TRIP}, vehicle {_VEHICLE}, timestamp BIGINT, delay INT, "
    f"stop_time_updates ARRAY<STRUCT<stop_sequence: INT, stop_id: STRING, arrival: {_EVENT}, "
    f"departure: {_EVENT}, schedule_relationship: STRING>>"
)

VEHICLE_POSITION = (
    f"{_ENVELOPE}, trip {_TRIP}, vehicle {_VEHICLE}, "
    "position STRUCT<latitude: DOUBLE, longitude: DOUBLE, bearing: DOUBLE, speed: DOUBLE>, "
    "current_stop_sequence INT, stop_id STRING, current_status STRING, timestamp BIGINT, "
    "congestion_level STRING, occupancy_status STRING"
)

HEARTBEAT = (
    "schema_version INT, fetch_id STRING, feed STRING, fetched_at_ms BIGINT, status STRING, "
    "http_status INT, elapsed_ms BIGINT, bytes BIGINT, feed_timestamp BIGINT, "
    "entity_count INT, records INT, entity_errors INT, archive_key STRING, archived BOOLEAN, "
    "kafka_delivered INT, kafka_unconfirmed INT, error STRING, retry_after_s DOUBLE, "
    "collected BOOLEAN"
)
