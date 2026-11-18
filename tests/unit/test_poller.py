"""The poller with a fake API, a fake Kafka and a real (temporary) archive directory."""

from __future__ import annotations

import itertools
import json
import threading
from collections.abc import Callable
from pathlib import Path

import jsonschema
import pytest

from ghostbus_core import schemas
from ghostbus_core.archive_layout import parse_snapshot_key
from ghostbus_core.feeds import MIN_CALL_SPACING_S, Feed
from ghostbus_core.pacing import FetchStatus
from ghostbus_ingest.archive import LocalArchive, decompress
from ghostbus_ingest.nta_client import FetchResult
from ghostbus_ingest.poller import poll_once, run
from tests import builders as b

T0_MS = 1_794_770_937_454


class FakeApi:
    def __init__(self, respond: Callable[[Feed], FetchResult]) -> None:
        self.respond = respond
        self.calls: list[Feed] = []

    def fetch(self, feed: Feed) -> FetchResult:
        self.calls.append(feed)
        return self.respond(feed)


def ok(payload: bytes) -> Callable[[Feed], FetchResult]:
    return lambda _feed: FetchResult(FetchStatus.OK, 200, payload, elapsed_ms=120)


class FakeKafka:
    def __init__(self, deliver: bool = True) -> None:
        self.deliver = deliver
        self.messages: list[tuple[str, str, dict[str, object]]] = []
        self._pending = 0

    def publish(self, topic: str, key: str, value: bytes) -> None:
        self.messages.append((topic, key, json.loads(value)))
        self._pending += 1

    def flush(self, timeout_s: float) -> tuple[int, int]:
        n, self._pending = self._pending, 0
        return (n, 0) if self.deliver else (0, n)

    def close(self) -> None:
        pass

    def on(self, topic: str) -> list[tuple[str, dict[str, object]]]:
        return [(k, v) for t, k, v in self.messages if t == topic]


class BrokenArchive(LocalArchive):
    def put_bytes(self, key: str, data: bytes, content_type: str) -> None:
        if "feed=heartbeat" not in key:
            raise OSError("disk full")
        super().put_bytes(key, data, content_type)


TRIPS = b.feed(b.scheduled_trip(), b.cancelled_trip(), b.added_trip()).SerializeToString()


def test_happy_path_archives_then_publishes(tmp_path: Path) -> None:
    archive, kafka = LocalArchive(tmp_path), FakeKafka()
    hb = poll_once(
        Feed.TRIP_UPDATES,
        client=FakeApi(ok(TRIPS)),
        archive=archive,
        publisher=kafka,
        now_ms=lambda: T0_MS,
    )
    assert hb.status is FetchStatus.OK and hb.collected
    assert hb.entity_count == hb.records == 3 and hb.entity_errors == 0
    assert hb.kafka_delivered == 3 and hb.kafka_unconfirmed == 0
    assert hb.feed_timestamp == b.HEADER_TS

    assert hb.archive_key is not None
    assert parse_snapshot_key(hb.archive_key) == (Feed.TRIP_UPDATES, T0_MS)
    stored = archive.get_bytes(hb.archive_key)
    assert stored is not None and decompress(stored) == TRIPS  # byte for byte

    trips = kafka.on("gtfsr.trip_updates.v1")
    assert [k for k, _ in trips] == [
        "20261115|6020_67195",
        "20261115|6020_65476",
        "20261115|2_133_e|0|16:15:00",
    ]
    schema = schemas.load(schemas.BY_TOPIC["gtfsr.trip_updates.v1"])
    for _, value in trips:
        jsonschema.validate(value, schema)
        assert value["fetch_id"] == hb.fetch_id

    ((beat_key, beat),) = kafka.on("ingest.heartbeat.v1")
    assert beat_key == "trip_updates" and beat["collected"] is True
    jsonschema.validate(beat, schemas.load(schemas.BY_TOPIC["ingest.heartbeat.v1"]))
    hb_files = list(archive.list_keys("raw/feed=heartbeat/"))
    assert len(hb_files) == 1
    assert json.loads(archive.get_bytes(hb_files[0]) or b"") == beat


def test_bad_entities_go_to_the_dlq(tmp_path: Path) -> None:
    mixed = b.feed(b.scheduled_trip(), b.vehicle("V1")).SerializeToString()
    kafka = FakeKafka()
    hb = poll_once(
        Feed.TRIP_UPDATES,
        client=FakeApi(ok(mixed)),
        archive=LocalArchive(tmp_path),
        publisher=kafka,
    )
    assert hb.records == 1 and hb.entity_errors == 1 and hb.collected
    assert [k for k, _ in kafka.on("gtfsr.trip_updates.v1.dlq")] == ["V1"]


def test_undecodable_payload_is_still_archived(tmp_path: Path) -> None:
    archive, kafka = LocalArchive(tmp_path), FakeKafka()
    hb = poll_once(
        Feed.VEHICLES, client=FakeApi(ok(b"\xff\xfe junk")), archive=archive, publisher=kafka
    )
    assert hb.status is FetchStatus.BAD_PAYLOAD and not hb.collected
    assert hb.archived and hb.error is not None and "parse" in hb.error
    assert kafka.on("gtfsr.vehicle_positions.v1") == []
    assert len(list(archive.list_keys("raw/feed=vehicles/"))) == 1


def test_throttled_call_writes_only_a_heartbeat(tmp_path: Path) -> None:
    api = FakeApi(
        lambda _f: FetchResult(FetchStatus.THROTTLED, 429, b"", 30, retry_after_s=60, error="slow")
    )
    archive, kafka = LocalArchive(tmp_path), FakeKafka()
    hb = poll_once(Feed.VEHICLES, client=api, archive=archive, publisher=kafka)
    assert hb.status is FetchStatus.THROTTLED and hb.http_status == 429 and not hb.archived
    assert hb.retry_after_s == 60
    assert list(archive.list_keys("raw/feed=vehicles/")) == []
    assert len(list(archive.list_keys("raw/feed=heartbeat/"))) == 1
    assert [t for t, _, _ in kafka.messages] == ["ingest.heartbeat.v1"]


def test_archive_failure_still_publishes_and_is_not_collected(tmp_path: Path) -> None:
    kafka = FakeKafka()
    hb = poll_once(
        Feed.TRIP_UPDATES,
        client=FakeApi(ok(TRIPS)),
        archive=BrokenArchive(tmp_path),
        publisher=kafka,
    )
    assert not hb.archived and not hb.collected
    assert hb.error is not None and "disk full" in hb.error
    assert len(kafka.on("gtfsr.trip_updates.v1")) == 3


def test_kafka_down_is_reported_not_fatal(tmp_path: Path) -> None:
    hb = poll_once(
        Feed.TRIP_UPDATES,
        client=FakeApi(ok(TRIPS)),
        archive=LocalArchive(tmp_path),
        publisher=FakeKafka(deliver=False),
    )
    assert hb.collected and hb.kafka_delivered == 0 and hb.kafka_unconfirmed == 3


# --- the scheduling loop ----------------------------------------------------------------------


class FakeClock:
    def __init__(self, start: float) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def wait(self, seconds: float) -> bool:
        self.now += seconds
        return False


def _run(
    tmp_path: Path, respond: Callable[[Feed], FetchResult], polls: int
) -> tuple[FakeApi, list[float]]:
    clock = FakeClock(start=29_912_840 * 60.0 + 3)
    api = FakeApi(respond)
    call_times: list[float] = []
    original = api.fetch

    def timed(feed: Feed) -> FetchResult:
        call_times.append(clock.now)
        clock.now += 1.5  # each call takes 1.5 s
        return original(feed)

    api.fetch = timed  # type: ignore[method-assign]
    run(
        client=api,
        archive=LocalArchive(tmp_path),
        publisher=FakeKafka(),
        offset_s=10.0,
        stop=threading.Event(),
        max_polls=polls,
        clock=clock,
        wait=clock.wait,
    )
    return api, call_times


def test_loop_makes_one_call_per_minute_and_rotates_feeds(tmp_path: Path) -> None:
    payload = b.feed(b.scheduled_trip()).SerializeToString()
    api, times = _run(tmp_path, ok(payload), polls=30)
    gaps = [b - a for a, b in itertools.pairwise(times)]
    assert min(gaps) >= MIN_CALL_SPACING_S
    assert all(t % 60 == pytest.approx(10.0) for t in times)
    assert api.calls.count(Feed.ALERTS) == 2
    assert api.calls.count(Feed.TRIP_UPDATES) == api.calls.count(Feed.VEHICLES) == 14


def test_loop_backs_off_on_repeated_server_errors(tmp_path: Path) -> None:
    def failing(_f: Feed) -> FetchResult:
        return FetchResult(FetchStatus.SERVER_ERROR, 503, b"", 50, error="down")

    _, times = _run(tmp_path, failing, polls=5)
    gaps_min = [round((b - a) / 60) for a, b in itertools.pairwise(times)]
    assert gaps_min == [1, 2, 4, 8]  # skip 0, 1, 3, 7 slots


def test_restarted_loop_respects_the_previous_process(tmp_path: Path) -> None:
    """A new process must not call within 60 s of a call archived by the one before it."""
    archive = LocalArchive(tmp_path)
    clock = FakeClock(start=29_912_840 * 60.0 + 3)
    previous_call = clock.now - 2  # the old process called 2 s ago, at second :01
    poll_once(
        Feed.VEHICLES,
        client=FakeApi(ok(TRIPS)),
        archive=archive,
        publisher=FakeKafka(),
        now_ms=lambda: round(previous_call * 1000),
    )
    calls: list[float] = []
    api = FakeApi(ok(TRIPS))
    original = api.fetch

    def timed(feed: Feed) -> FetchResult:
        calls.append(clock.now)
        return original(feed)

    api.fetch = timed  # type: ignore[method-assign]
    run(
        client=api,
        archive=archive,
        publisher=FakeKafka(),
        offset_s=5.0,  # :05 this minute would be only 4 s after the old call
        stop=threading.Event(),
        max_polls=1,
        clock=clock,
        wait=clock.wait,
    )
    assert calls[0] - previous_call >= MIN_CALL_SPACING_S
