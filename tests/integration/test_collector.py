"""The collector against real S3 (SeaweedFS) and real Kafka. Runs inside the compose stack."""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest
from confluent_kafka.admin import AdminClient

from ghostbus_core.feeds import Feed
from ghostbus_core.pacing import FetchStatus
from ghostbus_ingest.archive import S3Archive, archive_from_uri, decompress
from ghostbus_ingest.kafka_admin import ensure_topics
from ghostbus_ingest.kafka_config import admin_config
from ghostbus_ingest.nta_client import FetchResult
from ghostbus_ingest.poller import poll_once
from ghostbus_ingest.publisher import KafkaPublisher
from tests import builders as b

pytestmark = pytest.mark.integration

S3_ENDPOINT = os.environ.get("LAKE_S3_ENDPOINT", "http://localhost:8333")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")


class OneFeed:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def fetch(self, feed: Feed) -> FetchResult:
        return FetchResult(FetchStatus.OK, 200, self.payload, elapsed_ms=1)


@pytest.fixture
def s3() -> S3Archive:
    archive = archive_from_uri(f"s3://warehouse/it-archive/{uuid.uuid4().hex}", S3_ENDPOINT)
    assert isinstance(archive, S3Archive)
    return archive


def test_s3_archive_operations(s3: S3Archive, tmp_path: Path) -> None:
    s3.put_bytes("raw/feed=x/a.bin", b"1", "application/octet-stream")
    src = tmp_path / "z.zip"
    src.write_bytes(b"zip" * 1000)
    s3.put_file("raw/feed=x/sub/z.zip", src, "application/zip")
    assert s3.get_bytes("raw/feed=x/a.bin") == b"1"
    assert s3.create_if_absent("claims/minute=1", b"a")
    assert not s3.create_if_absent("claims/minute=1", b"b")  # conditional write lost
    assert s3.get_bytes("claims/minute=1") == b"a"
    assert s3.get_bytes("raw/feed=x/missing") is None
    assert s3.exists("raw/feed=x/a.bin") and not s3.exists("raw/feed=x/a")
    assert sorted(s3.list_keys("raw/feed=x/")) == ["raw/feed=x/a.bin", "raw/feed=x/sub/z.zip"]


def test_poll_once_into_s3_and_kafka(s3: S3Archive) -> None:
    # Own copies of the topics: this fixture once went to the live topics on the local stack and
    # reached the real silver tables (found by the Sprint 4 backfill comparison).
    prefix = f"it{uuid.uuid4().hex[:8]}."
    created = ensure_topics(BOOTSTRAP, replication_factor=1, prefix=prefix)
    assert created and all(name.startswith(prefix) for name in created)
    payload = b.feed(b.scheduled_trip(), b.cancelled_trip()).SerializeToString()
    publisher = KafkaPublisher(BOOTSTRAP, client_id="it-collector", topic_prefix=prefix)
    try:
        hb = poll_once(Feed.TRIP_UPDATES, client=OneFeed(payload), archive=s3, publisher=publisher)
    finally:
        publisher.close()
        AdminClient(admin_config(BOOTSTRAP)).delete_topics(created)
    assert hb.collected and hb.archive_key is not None
    assert hb.kafka_delivered == 2 and hb.kafka_unconfirmed == 0
    stored = s3.get_bytes(hb.archive_key)
    assert stored is not None and decompress(stored) == payload
    assert len(list(s3.list_keys("raw/feed=heartbeat/"))) == 1
