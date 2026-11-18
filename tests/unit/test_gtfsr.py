from __future__ import annotations

import base64

import jsonschema
import pytest
from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2 as pb

from ghostbus_core import schemas
from ghostbus_core.feeds import Feed
from ghostbus_core.gtfsr import FetchContext, dlq_value, normalize, parse_feed, trip_key
from tests import builders as b


def ctx(feed: Feed = Feed.TRIP_UPDATES) -> FetchContext:
    return FetchContext(
        feed=feed, fetch_id=f"{feed.value}:1794770401000", fetched_at_ms=1_794_770_401_000
    )


def validate(feed: Feed, value: dict[str, object]) -> None:
    jsonschema.validate(value, schemas.load(schemas.BY_TOPIC[feed.topic.name]))


def test_round_trip_through_bytes() -> None:
    msg = b.feed(b.scheduled_trip())
    assert parse_feed(msg.SerializeToString()) == msg


def test_garbage_is_a_decode_error() -> None:
    with pytest.raises(DecodeError):
        parse_feed(b"\xff\xff\xff not protobuf")


def test_scheduled_trip_keeps_zero_delay_distinct_from_unknown() -> None:
    out = normalize(ctx(), b.feed(b.scheduled_trip()))
    assert out.entity_count == 1 and not out.errors
    rec = out.records[0]
    assert rec.key == "20261115|6020_67195"
    stus = rec.value["stop_time_updates"]
    assert isinstance(stus, list)
    assert stus[0]["arrival"] == {"delay": 0, "time": None, "uncertainty": None}
    assert stus[1]["departure"] is None  # absent, not zero
    assert rec.value["delay"] is None
    assert rec.value["trip"] == {
        "trip_id": "6020_67195",
        "route_id": "2_64_d_a",
        "direction_id": 1,
        "start_time": "14:10:00",
        "start_date": "20261115",
        "schedule_relationship": "SCHEDULED",
    }
    validate(Feed.TRIP_UPDATES, rec.value)


def test_cancelled_trip() -> None:
    rec = normalize(ctx(), b.feed(b.cancelled_trip())).records[0]
    trip = rec.value["trip"]
    assert isinstance(trip, dict)
    assert trip["schedule_relationship"] == "CANCELED"
    assert rec.value["stop_time_updates"] == []
    assert rec.value["vehicle"] is None
    validate(Feed.TRIP_UPDATES, rec.value)


def test_added_trip_without_trip_id_gets_a_composite_key() -> None:
    rec = normalize(ctx(), b.feed(b.added_trip())).records[0]
    assert rec.key == "20261115|2_133_e|0|16:15:00"
    assert rec.value["trip_key"] == rec.key
    validate(Feed.TRIP_UPDATES, rec.value)


def test_entity_ids_are_not_used_as_keys() -> None:
    """NTA entity ids are positions in one snapshot; the same trip must get the same key."""
    a = normalize(ctx(), b.feed(b.scheduled_trip(entity_id="T1"))).records[0]
    c = normalize(ctx(), b.feed(b.scheduled_trip(entity_id="T999"))).records[0]
    assert a.key == c.key


def test_trip_key_fallback() -> None:
    assert trip_key(pb.TripDescriptor(), fallback="entity|X") == "entity|X"


def test_envelope_fields() -> None:
    rec = normalize(ctx(), b.feed(b.scheduled_trip())).records[0]
    assert rec.value["fetch_id"] == "trip_updates:1794770401000"
    assert rec.value["feed_timestamp"] == b.HEADER_TS
    assert rec.value["entity_id"] == "T1"
    assert rec.value["is_deleted"] is False


def test_missing_header_timestamp_is_null() -> None:
    out = normalize(ctx(), b.feed(b.scheduled_trip(), header_ts=None))
    assert out.header_timestamp is None
    assert out.records[0].value["feed_timestamp"] is None


def test_vehicle_positions() -> None:
    out = normalize(ctx(Feed.VEHICLES), b.feed(b.vehicle(), b.vehicle("V2", with_trip=False)))
    with_trip, without = out.records
    assert with_trip.key == "20261115|6020_67195"
    assert without.key == "vehicle|7139"
    assert without.value["trip"] is None
    assert with_trip.value["position"] == {
        "latitude": pytest.approx(53.35),
        "longitude": pytest.approx(-6.26),
        "bearing": None,
        "speed": None,
    }
    for r in out.records:
        validate(Feed.VEHICLES, r.value)


def test_alerts() -> None:
    rec = normalize(ctx(Feed.ALERTS), b.feed(b.alert())).records[0]
    assert rec.key == "alert|A1"
    assert rec.value["effect"] == "DETOUR"
    assert rec.value["active_periods"] == [{"start": b.HEADER_TS - 3600, "end": None}]
    assert rec.value["header_text"] == [{"language": "en", "text": "Diversion"}]
    validate(Feed.ALERTS, rec.value)


def test_wrong_entity_type_goes_to_errors_and_the_rest_survive() -> None:
    out = normalize(ctx(), b.feed(b.scheduled_trip(), b.vehicle("V9"), b.cancelled_trip()))
    assert len(out.records) == 2
    assert [e.entity_id for e in out.errors] == ["V9"]
    err = out.errors[0]
    assert "no trip_update" in err.error
    restored = pb.FeedEntity()
    restored.ParseFromString(base64.b64decode(err.raw_b64))
    assert restored.id == "V9"
    jsonschema.validate(dlq_value(ctx(), err), schemas.load(schemas.DLQ))


def test_empty_feed() -> None:
    out = normalize(ctx(), b.feed())
    assert out.entity_count == 0 and out.records == [] and out.errors == []
    assert out.incrementality == "FULL_DATASET"
