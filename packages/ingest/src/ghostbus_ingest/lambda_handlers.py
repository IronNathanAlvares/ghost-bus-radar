"""AWS Lambda entry points: the poller (fired every minute) and the static watcher (hourly).

The same ``poll_once`` and ``check_once`` as the local services; only the pacing differs. A
Lambda has no memory between invocations, so each one reads the newest heartbeats in the archive
to learn when this API key last called and whether it is backing off (ADR-0005, ADR-0009). That
also keeps the 60 s spacing across the move from the laptop poller to the cloud: the laptop's
last heartbeat is in the same archive.

Environment: ``ARCHIVE_URI`` (``s3://bucket/``), ``NTA_KEY_PARAMETER`` (SSM SecureString name),
``POLL_OFFSET_S`` (second of the minute to call at, default 15).
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from ghostbus_core.archive_layout import claim_key, hour_prefixes, parse_heartbeat_key
from ghostbus_core.feeds import feed_for_slot, minute_slot
from ghostbus_core.pacing import FetchStatus, backoff_slots, planned_call_at
from ghostbus_ingest.archive import Archive, archive_from_env
from ghostbus_ingest.nta_client import USER_AGENT, NtaClient
from ghostbus_ingest.poller import poll_once
from ghostbus_ingest.publisher import NullPublisher
from ghostbus_ingest.static_watcher import check_once

log = logging.getLogger("ghostbus.lambda")
log.setLevel(logging.INFO)

#: How far back to look for the last call. Backoff never exceeds 15 slots, so 30 min is plenty.
LOOKBACK = timedelta(minutes=30)
#: Read at most this many heartbeats to count consecutive failures (backoff caps at 15 slots).
MAX_FAILURES_READ = 6


@dataclass(frozen=True)
class CallState:
    last_call_s: float | None
    skip_slots: int


def call_state(archive: Archive, now_s: float) -> CallState:
    """When this key last called, and how many slots to skip after it, from the archive."""
    now = datetime.fromtimestamp(now_s, tz=UTC)
    keys = []
    for prefix in hour_prefixes("heartbeat", now - LOOKBACK, now + timedelta(minutes=1)):
        for key in archive.list_keys(prefix):
            try:
                _, ms = parse_heartbeat_key(key)
            except ValueError:
                continue
            keys.append((ms, key))
    if not keys:
        return CallState(None, 0)
    keys.sort(reverse=True)
    newest: dict[str, Any] | None = None
    failures = 0
    for _, key in keys[:MAX_FAILURES_READ]:
        raw = archive.get_bytes(key)
        hb = json.loads(raw) if raw else {}
        newest = newest or hb
        if hb.get("status") == FetchStatus.OK.value:
            break
        failures += 1
    assert newest is not None
    status = FetchStatus(newest.get("status", FetchStatus.OK.value))
    skip = backoff_slots(status, failures, newest.get("retry_after_s"))
    return CallState(keys[0][0] / 1000, skip)


_archive: Archive | None = None
_client: NtaClient | None = None


def _resources() -> tuple[Archive, NtaClient]:
    """Created once per container and reused across invocations."""
    global _archive, _client
    if _archive is None:
        _archive = archive_from_env(os.environ)
    if _client is None:
        import boto3

        ssm = boto3.client("ssm")
        name = os.environ["NTA_KEY_PARAMETER"]
        key = ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]
        _client = NtaClient(key.strip())
    return _archive, _client


def poll(
    event: dict[str, Any],
    context: Any,
    *,
    clock: Callable[[], float] = time.time,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """One scheduled minute: call the feed this minute belongs to, or skip it.

    The invocation owns the minute it starts in. If the plan (spacing, backoff, or a late start
    past the offset) puts the next call in a later minute, it skips; that minute's invocation
    will make the call.
    """
    archive, client = _resources()
    offset = float(os.environ.get("POLL_OFFSET_S", "15"))
    now = clock()
    state = call_state(archive, now)
    at = planned_call_at(now, offset, state.last_call_s, state.skip_slots)
    if minute_slot(at) != minute_slot(now):
        reason = "backoff" if state.skip_slots else "late or spacing"
        log.info("skip minute %d: next call planned at %.1f (%s)", minute_slot(now), at, reason)
        return {"called": False, "reason": reason, "next_call_at": at}
    slot = minute_slot(at)
    if not archive.create_if_absent(claim_key(slot), str(now).encode()):
        # EventBridge delivers at least once: another invocation already owns this minute.
        log.info("skip minute %d: already claimed", slot)
        return {"called": False, "reason": "claimed", "next_call_at": at}
    sleep(max(0.0, at - clock()))
    feed = feed_for_slot(slot)
    hb = poll_once(
        feed,
        client=client,
        archive=archive,
        publisher=NullPublisher(),
        now_ms=lambda: round(clock() * 1000),
    )
    out = {
        "called": True,
        "feed": feed.value,
        "status": hb.status.value,
        "collected": hb.collected,
        "entities": hb.entity_count,
        "elapsed_ms": hb.elapsed_ms,
    }
    # One line in CloudWatch Embedded Metric Format: a metric without a PutMetricData call.
    print(
        json.dumps(
            {
                "_aws": {
                    "Timestamp": int(at * 1000),
                    "CloudWatchMetrics": [
                        {
                            "Namespace": "GhostBus",
                            "Dimensions": [[]],
                            "Metrics": [{"Name": "Collected", "Unit": "Count"}],
                        }
                    ],
                },
                "Collected": int(hb.collected),
                **out,
            }
        )
    )
    return out


def watch_static(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Hourly: archive a new static GTFS version if NTA published one."""
    archive = archive_from_env(os.environ)
    timeout = httpx.Timeout(60.0, connect=15.0)
    with httpx.Client(
        headers={"User-Agent": USER_AGENT}, timeout=timeout, follow_redirects=True
    ) as http:
        res = check_once(http, archive)
    out = {
        "changed_headers": res.changed_headers,
        "new_version": res.new_version,
        "feed_version": res.feed_version,
        "sha256": res.sha256,
    }
    log.info("static check: %s", out)
    return out
