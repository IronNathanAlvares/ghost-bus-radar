"""Object keys in the raw archive (ADR-0001). The same layout locally and in S3.

::

    raw/feed=trip_updates/dt=2026-11-15/hh=19/20261115T192001.123Z.pb.zst   one snapshot
    raw/feed=heartbeat/dt=2026-11-15/hh=19/20261115T192001.123Z_trip_updates.json
    raw/feed=static/sha256=<64 hex>/GTFS_Realtime.zip                         one static version
    raw/feed=static/sha256=<64 hex>/manifest.json
    raw/feed=static/_state.json                                              watcher state

Partitions are by *fetch* time in UTC, which is known before the payload is parsed, so a broken
payload still lands in the right place. Keys sort in time order within a partition.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta

from ghostbus_core.feeds import Feed

RAW = "raw"
STATIC_STATE_KEY = f"{RAW}/feed=static/_state.json"
STATIC_FILENAME = "GTFS_Realtime.zip"

_SNAPSHOT_RE = re.compile(
    r"^raw/feed=(?P<feed>[a-z_]+)/dt=(?P<dt>\d{4}-\d{2}-\d{2})/hh=(?P<hh>\d{2})/"
    r"(?P<ts>\d{8}T\d{6}\.\d{3}Z)\.pb\.zst$"
)
_TS_FORMAT = "%Y%m%dT%H%M%S"


def _utc(epoch_ms: int) -> datetime:
    return datetime.fromtimestamp(epoch_ms / 1000, tz=UTC)


def _stamp(epoch_ms: int) -> str:
    return f"{_utc(epoch_ms).strftime(_TS_FORMAT)}.{epoch_ms % 1000:03d}Z"


def _partition(feed: str, epoch_ms: int) -> str:
    t = _utc(epoch_ms)
    return f"{RAW}/feed={feed}/dt={t:%Y-%m-%d}/hh={t:%H}"


def fetch_id(feed: Feed, fetched_at_ms: int) -> str:
    """Identifies one API call. Carried on every Kafka message and heartbeat it produced."""
    return f"{feed.value}:{fetched_at_ms}"


def snapshot_key(feed: Feed, fetched_at_ms: int) -> str:
    return f"{_partition(feed.value, fetched_at_ms)}/{_stamp(fetched_at_ms)}.pb.zst"


def heartbeat_key(feed: Feed, fetched_at_ms: int) -> str:
    return f"{_partition('heartbeat', fetched_at_ms)}/{_stamp(fetched_at_ms)}_{feed.value}.json"


def static_prefix(sha256: str) -> str:
    if not re.fullmatch(r"[0-9a-f]{64}", sha256):
        raise ValueError("sha256 must be 64 lowercase hex characters")
    return f"{RAW}/feed=static/sha256={sha256}/"


def claim_key(slot: int) -> str:
    """The marker one poller invocation writes to own minute ``slot`` (outside ``raw/``)."""
    return f"claims/minute={slot}"


def parse_snapshot_key(key: str) -> tuple[Feed, int]:
    """Inverse of :func:`snapshot_key`: the feed and fetch time (epoch ms) of an archived file."""
    m = _SNAPSHOT_RE.match(key)
    if not m:
        raise ValueError(f"not a snapshot key: {key!r}")
    return Feed(m["feed"]), _parse_stamp(m["ts"])


_HEARTBEAT_RE = re.compile(
    r"^raw/feed=heartbeat/dt=\d{4}-\d{2}-\d{2}/hh=\d{2}/"
    r"(?P<ts>\d{8}T\d{6}\.\d{3}Z)_(?P<feed>[a-z_]+)\.json$"
)


def _parse_stamp(stamp: str) -> int:
    seconds = datetime.strptime(stamp[:15], _TS_FORMAT).replace(tzinfo=UTC)
    return int(seconds.timestamp()) * 1000 + int(stamp[16:19])


def parse_heartbeat_key(key: str) -> tuple[Feed, int]:
    """Inverse of :func:`heartbeat_key`: the feed and call time (epoch ms) of a heartbeat."""
    m = _HEARTBEAT_RE.match(key)
    if not m:
        raise ValueError(f"not a heartbeat key: {key!r}")
    return Feed(m["feed"]), _parse_stamp(m["ts"])


def hour_prefixes(feed: str, start: datetime, end: datetime) -> list[str]:
    """Every hourly partition prefix overlapping ``[start, end)``, for listing a backfill range."""
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("start and end must be timezone-aware")
    t = start.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    end_utc = end.astimezone(UTC)
    out = []
    while t < end_utc:
        out.append(f"{RAW}/feed={feed}/dt={t:%Y-%m-%d}/hh={t:%H}/")
        t += timedelta(hours=1)
    return out
