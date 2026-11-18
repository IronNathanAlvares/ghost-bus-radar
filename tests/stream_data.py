"""Synthetic GTFS-R messages on throwaway Kafka topics, for the streaming tests."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from google.transit import gtfs_realtime_pb2 as pb

from ghostbus_core.archive_layout import fetch_id
from ghostbus_core.feeds import Feed
from ghostbus_core.gtfsr import FetchContext, normalize
from ghostbus_core.heartbeat import Heartbeat
from ghostbus_core.pacing import FetchStatus
from ghostbus_ingest.kafka_config import producer_config
from ghostbus_ingest.publisher import encode
from ghostbus_jobs.stream import Topics


@dataclass(frozen=True)
class Msg:
    topic: str
    key: str
    value: bytes


def make_topics(bootstrap: str, partitions: int = 3) -> Topics:
    prefix = f"it_{uuid.uuid4().hex[:8]}"
    topics = Topics(
        trip_updates=f"{prefix}.gtfsr.trip_updates.v1",
        vehicle_positions=f"{prefix}.gtfsr.vehicle_positions.v1",
        alerts=f"{prefix}.gtfsr.alerts.v1",
        heartbeat=f"{prefix}.ingest.heartbeat.v1",
    )
    admin = AdminClient({"bootstrap.servers": bootstrap})
    futures = admin.create_topics(
        [NewTopic(t, num_partitions=partitions, replication_factor=1) for t in topics.all()]
    )
    for f in futures.values():
        f.result()
    return topics


def produce(bootstrap: str, msgs: list[Msg]) -> None:
    p = Producer(producer_config(bootstrap, "it-stream"))
    for m in msgs:
        p.produce(m.topic, key=m.key, value=m.value)
    assert p.flush(30) == 0


def trip(entity_id: str, trip_id: str, n_stops: int, start: str = "08:00:00") -> pb.FeedEntity:
    e = pb.FeedEntity(id=entity_id)
    tu = e.trip_update
    tu.trip.trip_id = trip_id
    tu.trip.route_id = "2_64_d_a"
    tu.trip.direction_id = 0
    tu.trip.start_time = start
    tu.trip.start_date = "20261115"
    tu.trip.schedule_relationship = pb.TripDescriptor.SCHEDULED
    for i in range(n_stops):
        s = tu.stop_time_update.add(stop_sequence=i + 1, stop_id=f"STOP{i}")
        s.arrival.delay = 30 * i
    return e


def vehicle(entity_id: str, vehicle_id: str, trip_id: str, ts: int) -> pb.FeedEntity:
    e = pb.FeedEntity(id=entity_id)
    vp = e.vehicle
    vp.trip.trip_id = trip_id
    vp.trip.start_date = "20261115"
    vp.vehicle.id = vehicle_id
    vp.position.latitude = 53.35
    vp.position.longitude = -6.26
    vp.timestamp = ts
    return e


def snapshot(
    feed: Feed, topics: Topics, fetched_at_s: int, entities: list[pb.FeedEntity]
) -> list[Msg]:
    """One poll: the entity messages plus its heartbeat, exactly as the poller publishes them."""
    msg = pb.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = fetched_at_s
    msg.entity.extend(entities)
    ms = fetched_at_s * 1000
    ctx = FetchContext(feed=feed, fetch_id=fetch_id(feed, ms), fetched_at_ms=ms)
    norm = normalize(ctx, msg)
    topic = topics.trip_updates if feed is Feed.TRIP_UPDATES else topics.vehicle_positions
    out = [Msg(topic, r.key, encode(r.value)) for r in norm.records]
    hb = Heartbeat(
        fetch_id=ctx.fetch_id,
        feed=feed,
        fetched_at_ms=ms,
        status=FetchStatus.OK,
        http_status=200,
        elapsed_ms=150,
        bytes=1000,
        feed_timestamp=fetched_at_s,
        entity_count=len(entities),
        records=len(norm.records),
        entity_errors=0,
        archive_key=None,
        archived=True,
        kafka_delivered=len(norm.records),
        kafka_unconfirmed=0,
        error=None,
    )
    out.append(Msg(topics.heartbeat, feed.value, encode(hb.to_json())))
    return out
