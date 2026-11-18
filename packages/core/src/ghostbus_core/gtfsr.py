"""GTFS-Realtime protobuf to plain JSON records, one per feed entity.

This is the only code that knows the protobuf layout. The poller uses it to publish to Kafka; the
backfill job will use it to replay the raw archive, so both paths produce identical records.

Rules:

* GTFS-RT is proto2, so "absent" and "zero" are different. An absent optional field becomes
  ``None``, never ``0`` or ``""``. A delay of 0 s means on time; a missing delay means unknown.
* Enums become their names (``"CANCELED"``), which survive proto renumbering and read well in SQL.
* NTA entity ids (``T1``, ``T2``, ...) are positions in one snapshot, not stable ids, so nothing
  downstream may dedupe on them. Records carry a ``trip_key`` instead (see :func:`trip_key`).
* One bad entity never sinks the snapshot: it comes back as an :class:`EntityError` for the
  dead-letter topic and the rest are published.

Output shapes are pinned by JSON Schemas in ``ghostbus_core/schemas/``.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass, field

from google.protobuf.message import Message
from google.transit import gtfs_realtime_pb2 as pb

from ghostbus_core.feeds import Feed

SCHEMA_VERSION = 1

Json = dict[str, object]


@dataclass(frozen=True)
class FetchContext:
    """What the poller knew about the call that produced a snapshot."""

    feed: Feed
    fetch_id: str
    fetched_at_ms: int


@dataclass(frozen=True)
class Record:
    key: str
    value: Json


@dataclass(frozen=True)
class EntityError:
    entity_id: str
    error: str
    raw_b64: str


@dataclass(frozen=True)
class NormalizedFeed:
    header_timestamp: int | None
    gtfs_realtime_version: str
    incrementality: str
    entity_count: int
    records: list[Record] = field(default_factory=list)
    errors: list[EntityError] = field(default_factory=list)


def parse_feed(payload: bytes) -> pb.FeedMessage:
    """Decode a FeedMessage. Raises ``google.protobuf.message.DecodeError`` on bad bytes."""
    msg = pb.FeedMessage()
    msg.ParseFromString(payload)
    return msg


def _opt(msg: Message, name: str) -> object | None:
    """The field's value if it is set, else None (proto2 presence)."""
    return getattr(msg, name) if msg.HasField(name) else None


def _enum(msg: Message, name: str) -> str | None:
    if not msg.HasField(name):
        return None
    enum_type = msg.DESCRIPTOR.fields_by_name[name].enum_type
    if enum_type is None:
        raise TypeError(f"{name} is not an enum field")
    return str(enum_type.values_by_number[getattr(msg, name)].name)


def _trip(t: pb.TripDescriptor) -> Json:
    return {
        "trip_id": _opt(t, "trip_id"),
        "route_id": _opt(t, "route_id"),
        "direction_id": _opt(t, "direction_id"),
        "start_time": _opt(t, "start_time"),
        "start_date": _opt(t, "start_date"),
        "schedule_relationship": _enum(t, "schedule_relationship"),
    }


def _vehicle(v: pb.VehicleDescriptor) -> Json:
    return {
        "id": _opt(v, "id"),
        "label": _opt(v, "label"),
        "license_plate": _opt(v, "license_plate"),
    }


def _stop_time_event(e: pb.TripUpdate.StopTimeEvent) -> Json:
    return {
        "delay": _opt(e, "delay"),
        "time": _opt(e, "time"),
        "uncertainty": _opt(e, "uncertainty"),
    }


def trip_key(trip: pb.TripDescriptor, fallback: str) -> str:
    """Kafka key and dedupe key for one trip instance *within one static feed version*.

    ``start_date|trip_id`` when the trip has an id. ADDED trips have none, so they use
    ``start_date|route_id|direction_id|start_time``. The durable natural key (ADR-0002) needs the
    static timetable and is derived in Sprint 2; this key only has to keep one trip's messages on
    one partition, in order.
    """
    date = trip.start_date if trip.HasField("start_date") else ""
    if trip.HasField("trip_id"):
        return f"{date}|{trip.trip_id}"
    if trip.HasField("route_id") and trip.HasField("start_time"):
        direction = str(trip.direction_id) if trip.HasField("direction_id") else ""
        return f"{date}|{trip.route_id}|{direction}|{trip.start_time}"
    return fallback


def _envelope(ctx: FetchContext, header_ts: int | None, entity: pb.FeedEntity) -> Json:
    return {
        "schema_version": SCHEMA_VERSION,
        "fetch_id": ctx.fetch_id,
        "fetched_at_ms": ctx.fetched_at_ms,
        "feed_timestamp": header_ts,
        "entity_id": entity.id,
        "is_deleted": entity.is_deleted if entity.HasField("is_deleted") else False,
    }


def _trip_update(ctx: FetchContext, header_ts: int | None, e: pb.FeedEntity) -> Record:
    tu = e.trip_update
    key = trip_key(tu.trip, fallback=f"entity|{e.id}")
    value = _envelope(ctx, header_ts, e) | {
        "trip_key": key,
        "trip": _trip(tu.trip),
        "vehicle": _vehicle(tu.vehicle) if tu.HasField("vehicle") else None,
        "timestamp": _opt(tu, "timestamp"),
        "delay": _opt(tu, "delay"),
        "stop_time_updates": [
            {
                "stop_sequence": _opt(s, "stop_sequence"),
                "stop_id": _opt(s, "stop_id"),
                "arrival": _stop_time_event(s.arrival) if s.HasField("arrival") else None,
                "departure": _stop_time_event(s.departure) if s.HasField("departure") else None,
                "schedule_relationship": _enum(s, "schedule_relationship"),
            }
            for s in tu.stop_time_update
        ],
    }
    return Record(key=key, value=value)


def _vehicle_position(ctx: FetchContext, header_ts: int | None, e: pb.FeedEntity) -> Record:
    vp = e.vehicle
    vehicle_id = vp.vehicle.id if vp.HasField("vehicle") and vp.vehicle.HasField("id") else None
    fallback = f"vehicle|{vehicle_id}" if vehicle_id else f"entity|{e.id}"
    key = trip_key(vp.trip, fallback=fallback) if vp.HasField("trip") else fallback
    pos = vp.position
    value = _envelope(ctx, header_ts, e) | {
        "trip_key": key,
        "trip": _trip(vp.trip) if vp.HasField("trip") else None,
        "vehicle": _vehicle(vp.vehicle) if vp.HasField("vehicle") else None,
        "position": {
            "latitude": pos.latitude,
            "longitude": pos.longitude,
            "bearing": _opt(pos, "bearing"),
            "speed": _opt(pos, "speed"),
        }
        if vp.HasField("position")
        else None,
        "current_stop_sequence": _opt(vp, "current_stop_sequence"),
        "stop_id": _opt(vp, "stop_id"),
        "current_status": _enum(vp, "current_status"),
        "timestamp": _opt(vp, "timestamp"),
        "congestion_level": _enum(vp, "congestion_level"),
        "occupancy_status": _enum(vp, "occupancy_status"),
    }
    return Record(key=key, value=value)


def _translations(ts: pb.TranslatedString) -> list[Json]:
    return [{"language": _opt(t, "language"), "text": t.text} for t in ts.translation]


def _alert(ctx: FetchContext, header_ts: int | None, e: pb.FeedEntity) -> Record:
    a = e.alert
    value = _envelope(ctx, header_ts, e) | {
        "active_periods": [
            {"start": _opt(p, "start"), "end": _opt(p, "end")} for p in a.active_period
        ],
        "informed_entities": [
            {
                "agency_id": _opt(s, "agency_id"),
                "route_id": _opt(s, "route_id"),
                "route_type": _opt(s, "route_type"),
                "direction_id": _opt(s, "direction_id"),
                "trip": _trip(s.trip) if s.HasField("trip") else None,
                "stop_id": _opt(s, "stop_id"),
            }
            for s in a.informed_entity
        ],
        "cause": _enum(a, "cause"),
        "effect": _enum(a, "effect"),
        "severity_level": _enum(a, "severity_level"),
        "url": _translations(a.url),
        "header_text": _translations(a.header_text),
        "description_text": _translations(a.description_text),
    }
    return Record(key=f"alert|{e.id}", value=value)


_EXPECTED = {Feed.TRIP_UPDATES: "trip_update", Feed.VEHICLES: "vehicle", Feed.ALERTS: "alert"}
_BUILDERS = {Feed.TRIP_UPDATES: _trip_update, Feed.VEHICLES: _vehicle_position, Feed.ALERTS: _alert}


def normalize(ctx: FetchContext, msg: pb.FeedMessage) -> NormalizedFeed:
    """Turn one snapshot into records for ``ctx.feed``'s topic, plus per-entity errors."""
    header_ts = msg.header.timestamp if msg.header.HasField("timestamp") else None
    out = NormalizedFeed(
        header_timestamp=header_ts,
        gtfs_realtime_version=msg.header.gtfs_realtime_version,
        incrementality=_enum(msg.header, "incrementality") or "FULL_DATASET",
        entity_count=len(msg.entity),
    )
    expected = _EXPECTED[ctx.feed]
    build = _BUILDERS[ctx.feed]
    for entity in msg.entity:
        try:
            if not entity.HasField(expected):
                raise ValueError(f"entity has no {expected}")
            out.records.append(build(ctx, header_ts, entity))
        except Exception as exc:  # one bad entity must not sink the snapshot
            out.errors.append(
                EntityError(
                    entity_id=entity.id,
                    error=f"{type(exc).__name__}: {exc}",
                    raw_b64=base64.b64encode(entity.SerializeToString()).decode("ascii"),
                )
            )
    return out


def dlq_value(ctx: FetchContext, err: EntityError) -> Json:
    return {
        "schema_version": SCHEMA_VERSION,
        "fetch_id": ctx.fetch_id,
        "fetched_at_ms": ctx.fetched_at_ms,
        "feed": ctx.feed.value,
        "entity_id": err.entity_id,
        "error": err.error,
        "raw_b64": err.raw_b64,
    }
