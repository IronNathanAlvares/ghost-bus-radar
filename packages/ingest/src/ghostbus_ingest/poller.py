"""The GTFS-R poller: one API call per minute, archive first, then Kafka, then a heartbeat.

Usage::

    python -m ghostbus_ingest.poller                   # run forever (Docker: the `poller` service)
    python -m ghostbus_ingest.poller --max-polls 3     # three scheduled calls, then exit
    python -m ghostbus_ingest.poller --once vehicles   # a single call now, for debugging

Environment: ``NTA_API_KEY`` (required), ``ARCHIVE_URI`` (required), ``ARCHIVE_S3_ENDPOINT``,
``KAFKA_BOOTSTRAP`` (empty means archive only), ``POLL_OFFSET_S`` (second of the minute to call
at; random 5-25 when unset).

Only one poller may run per API key, anywhere: two would break the one-call-per-minute limit.
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import signal
import sys
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from types import FrameType

from google.protobuf.message import DecodeError

from ghostbus_core.archive_layout import (
    fetch_id,
    heartbeat_key,
    hour_prefixes,
    parse_heartbeat_key,
    snapshot_key,
)
from ghostbus_core.feeds import Feed, feed_for_slot, minute_slot
from ghostbus_core.gtfsr import FetchContext, dlq_value, normalize, parse_feed
from ghostbus_core.heartbeat import Heartbeat
from ghostbus_core.pacing import FetchStatus, backoff_slots, next_call_at
from ghostbus_core.topics import HEARTBEAT
from ghostbus_ingest.archive import Archive, archive_from_env, compress
from ghostbus_ingest.nta_client import FeedFetcher, NtaClient
from ghostbus_ingest.publisher import KafkaPublisher, NullPublisher, Publisher, encode

log = logging.getLogger("ghostbus.poller")

KAFKA_FLUSH_TIMEOUT_S = 15.0


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def poll_once(
    feed: Feed,
    *,
    client: FeedFetcher,
    archive: Archive,
    publisher: Publisher,
    now_ms: Callable[[], int] = _now_ms,
) -> Heartbeat:
    """One call: fetch, archive the raw bytes, publish entities, write a heartbeat."""
    fetched_at_ms = now_ms()
    fid = fetch_id(feed, fetched_at_ms)
    res = client.fetch(feed)

    status = res.status
    errors: list[str] = [res.error] if res.error else []
    archive_key: str | None = None
    archived = False
    feed_ts: int | None = None
    entity_count: int | None = None
    n_records = n_entity_errors = 0
    delivered = unconfirmed = 0

    if status is FetchStatus.OK:
        key = snapshot_key(feed, fetched_at_ms)
        try:  # the archive write comes first and is what makes a snapshot "collected"
            archive.put_bytes(key, compress(res.body), "application/zstd")
            archive_key, archived = key, True
        except Exception as exc:
            log.exception("archive write failed for %s", key)
            errors.append(f"archive: {type(exc).__name__}: {exc}")

        try:
            msg = parse_feed(res.body)
        except DecodeError as exc:
            status = FetchStatus.BAD_PAYLOAD
            errors.append(f"parse: {exc}")
        else:
            ctx = FetchContext(feed=feed, fetch_id=fid, fetched_at_ms=fetched_at_ms)
            norm = normalize(ctx, msg)
            feed_ts, entity_count = norm.header_timestamp, norm.entity_count
            n_records, n_entity_errors = len(norm.records), len(norm.errors)
            topic = feed.topic
            for rec in norm.records:
                publisher.publish(topic.name, rec.key, encode(rec.value))
            for err in norm.errors:
                publisher.publish(topic.dlq().name, err.entity_id, encode(dlq_value(ctx, err)))
            delivered, unconfirmed = publisher.flush(KAFKA_FLUSH_TIMEOUT_S)

    hb = Heartbeat(
        fetch_id=fid,
        feed=feed,
        fetched_at_ms=fetched_at_ms,
        status=status,
        http_status=res.http_status,
        elapsed_ms=res.elapsed_ms,
        bytes=len(res.body),
        feed_timestamp=feed_ts,
        entity_count=entity_count,
        records=n_records,
        entity_errors=n_entity_errors,
        archive_key=archive_key,
        archived=archived,
        kafka_delivered=delivered,
        kafka_unconfirmed=unconfirmed,
        error="; ".join(errors)[:500] or None,
        retry_after_s=res.retry_after_s,
    )
    body = encode(hb.to_json())
    try:
        archive.put_bytes(heartbeat_key(feed, fetched_at_ms), body, "application/json")
    except Exception:
        log.exception("heartbeat archive write failed")
    publisher.publish(HEARTBEAT.name, feed.value, body)
    publisher.flush(5.0)
    return hb


def last_call_from_archive(archive: Archive, now_s: float) -> float | None:
    """When this key last called the API, from the newest heartbeat in the last two hours.

    A restarted poller has no memory, and without this its first call could land within 60 s of
    the previous process's last one.
    """
    now = datetime.fromtimestamp(now_s, tz=UTC)
    latest: int | None = None
    for prefix in hour_prefixes("heartbeat", now - timedelta(hours=2), now + timedelta(hours=1)):
        for key in archive.list_keys(prefix):
            try:
                _, ms = parse_heartbeat_key(key)
            except ValueError:
                continue
            latest = ms if latest is None else max(latest, ms)
    return None if latest is None else latest / 1000


def run(
    *,
    client: FeedFetcher,
    archive: Archive,
    publisher: Publisher,
    offset_s: float,
    stop: threading.Event,
    max_polls: int | None = None,
    clock: Callable[[], float] = time.time,
    wait: Callable[[float], bool] | None = None,
) -> int:
    """Poll on the minute schedule until ``stop`` is set. Returns the number of calls made.

    ``wait(seconds)`` sleeps and returns True if asked to stop; tests pass a fake clock.
    """
    wait = wait or stop.wait
    last_call = last_call_from_archive(archive, clock())
    if last_call is not None:
        log.info("last call by any poller on this archive: %.0f s ago", clock() - last_call)
    failures = skip = polls = 0
    while not stop.is_set() and (max_polls is None or polls < max_polls):
        at = next_call_at(clock(), offset_s, last_call, skip)
        if wait(max(0.0, at - clock())):
            break
        now = clock()
        feed = feed_for_slot(minute_slot(now))
        last_call = now
        hb = poll_once(
            feed,
            client=client,
            archive=archive,
            publisher=publisher,
            now_ms=lambda: round(clock() * 1000),
        )
        polls += 1
        failures = 0 if hb.status is FetchStatus.OK else failures + 1
        skip = backoff_slots(hb.status, failures, hb.retry_after_s)
        lag = (
            f"{hb.fetched_at_ms / 1000 - hb.feed_timestamp:.0f}s"
            if hb.feed_timestamp is not None
            else "-"
        )
        log.info(
            "%-12s %-13s http=%s %6d ms %8d B entities=%s lag=%s archived=%s kafka=%d/%d%s%s",
            feed.value,
            hb.status.value,
            hb.http_status,
            hb.elapsed_ms,
            hb.bytes,
            hb.entity_count,
            lag,
            hb.archived,
            hb.kafka_delivered,
            hb.records + hb.entity_errors,
            f" skip={skip}" if skip else "",
            f" error={hb.error}" if hb.error else "",
        )
    return polls


def _publisher_from_env() -> Publisher:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "").strip()
    if not bootstrap:
        log.info("KAFKA_BOOTSTRAP empty: archive-only (collect) mode")
        return NullPublisher()
    return KafkaPublisher(bootstrap)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", choices=[f.value for f in Feed], help="one call now, then exit")
    parser.add_argument("--max-polls", type=int, help="stop after this many scheduled calls")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)

    client = NtaClient(os.environ.get("NTA_API_KEY", "").strip())
    archive = archive_from_env(os.environ)
    publisher = _publisher_from_env()
    log.info("archive=%s", archive.uri)

    try:
        if args.once:
            hb = poll_once(Feed(args.once), client=client, archive=archive, publisher=publisher)
            print(hb.to_json())
            return 0 if hb.collected else 1

        offset_env = os.environ.get("POLL_OFFSET_S")
        offset_s = float(offset_env) if offset_env else random.uniform(5, 25)
        log.info("calling at second %.1f of each minute; one call per minute", offset_s)
        stop = threading.Event()

        def _stop(signum: int, _frame: FrameType | None) -> None:
            log.info("signal %d: stopping after the current call", signum)
            stop.set()

        signal.signal(signal.SIGTERM, _stop)
        signal.signal(signal.SIGINT, _stop)
        run(
            client=client,
            archive=archive,
            publisher=publisher,
            offset_s=offset_s,
            stop=stop,
            max_polls=args.max_polls,
        )
        return 0
    finally:
        publisher.close()
        client.close()


if __name__ == "__main__":
    sys.exit(main())
