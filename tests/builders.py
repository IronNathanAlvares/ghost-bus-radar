"""Synthetic GTFS-R feeds for tests, shaped like what the NTA v2 API actually returns.

Synthetic rather than recorded so tests carry no third-party data and every edge case is explicit.
"""

from __future__ import annotations

from google.transit import gtfs_realtime_pb2 as pb

HEADER_TS = 1_794_770_401


def feed(*entities: pb.FeedEntity, header_ts: int | None = HEADER_TS) -> pb.FeedMessage:
    msg = pb.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.incrementality = pb.FeedHeader.FULL_DATASET
    if header_ts is not None:
        msg.header.timestamp = header_ts
    msg.entity.extend(entities)
    return msg


def scheduled_trip(entity_id: str = "T1", trip_id: str = "6020_67195") -> pb.FeedEntity:
    """A running trip: two stop predictions, one with a zero delay (on time, not unknown)."""
    e = pb.FeedEntity(id=entity_id)
    tu = e.trip_update
    tu.trip.trip_id = trip_id
    tu.trip.route_id = "2_64_d_a"
    tu.trip.direction_id = 1
    tu.trip.start_time = "14:10:00"
    tu.trip.start_date = "20261115"
    tu.trip.schedule_relationship = pb.TripDescriptor.SCHEDULED
    tu.vehicle.id = "7139"
    tu.timestamp = HEADER_TS
    s1 = tu.stop_time_update.add(stop_sequence=25, stop_id="8530B6358301")
    s1.arrival.delay = 0
    s1.departure.delay = 0
    s1.schedule_relationship = pb.TripUpdate.StopTimeUpdate.SCHEDULED
    s2 = tu.stop_time_update.add(stop_sequence=26, stop_id="8530B158221")
    s2.arrival.delay = 1639
    return e


def cancelled_trip(entity_id: str = "T115") -> pb.FeedEntity:
    e = pb.FeedEntity(id=entity_id)
    t = e.trip_update.trip
    t.trip_id = "6020_65476"
    t.route_id = "2_466_c_a"
    t.direction_id = 1
    t.start_time = "17:30:00"
    t.start_date = "20261115"
    t.schedule_relationship = pb.TripDescriptor.CANCELED
    e.trip_update.timestamp = HEADER_TS
    return e


def added_trip(entity_id: str = "T16") -> pb.FeedEntity:
    """NTA ADDED trips carry no trip_id."""
    e = pb.FeedEntity(id=entity_id)
    tu = e.trip_update
    tu.trip.route_id = "2_133_e"
    tu.trip.direction_id = 0
    tu.trip.start_time = "16:15:00"
    tu.trip.start_date = "20261115"
    tu.trip.schedule_relationship = pb.TripDescriptor.ADDED
    s = tu.stop_time_update.add(stop_sequence=27, stop_id="8350B3202601")
    s.arrival.time = 1_794_763_320
    return e


def vehicle(entity_id: str = "V1", *, with_trip: bool = True) -> pb.FeedEntity:
    e = pb.FeedEntity(id=entity_id)
    vp = e.vehicle
    if with_trip:
        vp.trip.trip_id = "6020_67195"
        vp.trip.start_date = "20261115"
        vp.trip.schedule_relationship = pb.TripDescriptor.SCHEDULED
    vp.vehicle.id = "7139"
    vp.position.latitude = 53.35
    vp.position.longitude = -6.26
    vp.timestamp = HEADER_TS - 20
    return e


def alert(entity_id: str = "A1") -> pb.FeedEntity:
    e = pb.FeedEntity(id=entity_id)
    a = e.alert
    a.active_period.add(start=HEADER_TS - 3600)
    a.informed_entity.add(route_id="2_64_d_a")
    a.cause = pb.Alert.CONSTRUCTION
    a.effect = pb.Alert.DETOUR
    a.header_text.translation.add(text="Diversion", language="en")
    return e
