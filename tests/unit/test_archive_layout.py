from __future__ import annotations

from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import pytest
from hypothesis import given
from hypothesis import strategies as st

from ghostbus_core.archive_layout import (
    fetch_id,
    heartbeat_key,
    hour_prefixes,
    parse_heartbeat_key,
    parse_snapshot_key,
    snapshot_key,
    static_prefix,
)
from ghostbus_core.feeds import Feed

MS = 1_794_770_937_454  # 2026-11-15T19:28:57.454Z


def test_snapshot_key() -> None:
    assert snapshot_key(Feed.VEHICLES, MS) == (
        "raw/feed=vehicles/dt=2026-11-15/hh=19/20261115T192857.454Z.pb.zst"
    )


def test_heartbeat_key() -> None:
    assert heartbeat_key(Feed.TRIP_UPDATES, MS) == (
        "raw/feed=heartbeat/dt=2026-11-15/hh=19/20261115T192857.454Z_trip_updates.json"
    )


def test_fetch_id() -> None:
    assert fetch_id(Feed.ALERTS, MS) == "alerts:1794770937454"


@given(st.sampled_from(list(Feed)), st.integers(min_value=0, max_value=4_102_444_799_999))
def test_snapshot_key_round_trips(feed: Feed, ms: int) -> None:
    assert parse_snapshot_key(snapshot_key(feed, ms)) == (feed, ms)


@given(st.integers(min_value=0, max_value=4_102_444_700_000), st.integers(1, 100_000))
def test_snapshot_keys_sort_in_time_order(ms: int, delta: int) -> None:
    assert snapshot_key(Feed.VEHICLES, ms) < snapshot_key(Feed.VEHICLES, ms + delta)


def test_parse_rejects_other_keys() -> None:
    with pytest.raises(ValueError):
        parse_snapshot_key("raw/feed=vehicles/dt=2026-11-15/hh=19/notes.txt")


def test_static_prefix_validates_hash() -> None:
    assert static_prefix("a" * 64) == f"raw/feed=static/sha256={'a' * 64}/"
    with pytest.raises(ValueError):
        static_prefix("ABC")


def test_hour_prefixes_cross_midnight() -> None:
    start = datetime(2026, 11, 15, 22, 30, tzinfo=UTC)
    end = datetime(2026, 11, 16, 1, 0, tzinfo=UTC)
    assert hour_prefixes("vehicles", start, end) == [
        "raw/feed=vehicles/dt=2026-11-15/hh=22/",
        "raw/feed=vehicles/dt=2026-11-15/hh=23/",
        "raw/feed=vehicles/dt=2026-11-16/hh=00/",
    ]


def test_hour_prefixes_over_the_autumn_clock_change_are_utc() -> None:
    """Dublin repeats 01:00-02:00 on 25 Oct 2026; the archive is UTC, so no hour is lost."""
    dublin = ZoneInfo("Europe/Dublin")
    start = datetime(2026, 10, 25, 0, 0, tzinfo=dublin)  # 23:00 UTC on the 24th
    end = datetime(2026, 10, 25, 3, 0, tzinfo=dublin)  # 03:00 UTC
    assert len(hour_prefixes("trip_updates", start, end)) == 4


def test_hour_prefixes_need_aware_datetimes() -> None:
    with pytest.raises(ValueError):
        hour_prefixes("vehicles", datetime(2026, 1, 1), datetime(2026, 1, 2))  # noqa: DTZ001


@given(st.sampled_from(list(Feed)), st.integers(min_value=0, max_value=4_102_444_799_999))
def test_heartbeat_key_round_trips(feed: Feed, ms: int) -> None:
    assert parse_heartbeat_key(heartbeat_key(feed, ms)) == (feed, ms)
