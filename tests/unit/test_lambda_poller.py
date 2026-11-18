"""The stateless Lambda poller keeps every pacing rule of the long-running one (ADR-0005).

Each test drives ``lambda_handlers.poll`` minute by minute with a fake clock, the way
EventBridge Scheduler would, against a local archive and a fake API.
"""

from __future__ import annotations

import itertools
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from ghostbus_core.feeds import MIN_CALL_SPACING_S, Feed
from ghostbus_core.pacing import FetchStatus
from ghostbus_ingest import lambda_handlers as lh
from ghostbus_ingest.archive import LocalArchive
from ghostbus_ingest.nta_client import FetchResult
from ghostbus_ingest.poller import poll_once
from ghostbus_ingest.publisher import NullPublisher
from tests import builders as b

MINUTE0 = 29_913_840 * 60.0  # a whole minute, 2026-11-15
PAYLOAD = b.feed(b.scheduled_trip()).SerializeToString()


class Clock:
    def __init__(self, now: float) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


class Api:
    def __init__(self, clock: Clock, respond: Callable[[Feed], FetchResult]) -> None:
        self.clock, self.respond = clock, respond
        self.calls: list[tuple[float, Feed]] = []

    def fetch(self, feed: Feed) -> FetchResult:
        self.calls.append((self.clock.now, feed))
        self.clock.now += 1.2  # a call takes 1.2 s
        return self.respond(feed)


def ok(_feed: Feed) -> FetchResult:
    return FetchResult(FetchStatus.OK, 200, PAYLOAD, elapsed_ms=1200)


def down(_feed: Feed) -> FetchResult:
    return FetchResult(FetchStatus.SERVER_ERROR, 503, b"", 50, error="down")


@pytest.fixture
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[LocalArchive]:
    archive = LocalArchive(tmp_path)
    monkeypatch.setenv("POLL_OFFSET_S", "15")
    monkeypatch.setattr(lh, "_archive", archive)
    yield archive


def invoke_each_minute(
    archive: LocalArchive,
    respond: Callable[[Feed], FetchResult],
    minutes: int,
    start_second: float = 2.0,
    late: Callable[[int], float] = lambda _m: 0.0,
) -> Api:
    clock = Clock(MINUTE0)
    api = Api(clock, respond)
    lh._client = api  # type: ignore[assignment]
    for m in range(minutes):
        clock.now = max(clock.now, MINUTE0 + m * 60 + start_second + late(m))
        lh.poll({}, None, clock=clock, sleep=clock.sleep)
    return api


def test_one_call_per_minute_on_the_offset_and_feeds_rotate(env: LocalArchive) -> None:
    api = invoke_each_minute(env, ok, minutes=30)
    times = [t for t, _ in api.calls]
    assert len(times) == 30
    assert all(t % 60 == pytest.approx(15.0) for t in times)
    assert min(b - a for a, b in itertools.pairwise(times)) >= MIN_CALL_SPACING_S
    feeds = [f for _, f in api.calls]
    assert feeds.count(Feed.ALERTS) == 2
    assert feeds.count(Feed.TRIP_UPDATES) == feeds.count(Feed.VEHICLES) == 14


def test_a_late_invocation_skips_its_minute(env: LocalArchive) -> None:
    """Started after the offset (cold start, scheduler delay): no squeezed-in second call."""
    api = invoke_each_minute(env, ok, minutes=5, late=lambda m: 20.0 if m == 2 else 0.0)
    minutes = [int((t - MINUTE0) // 60) for t, _ in api.calls]
    assert minutes == [0, 1, 3, 4]


def test_a_duplicate_delivery_in_the_same_minute_does_nothing(env: LocalArchive) -> None:
    """EventBridge delivers at least once; a second invocation must not make a second call."""
    clock = Clock(MINUTE0 + 2)
    api = Api(clock, ok)
    lh._client = api  # type: ignore[assignment]
    first = lh.poll({}, None, clock=clock, sleep=clock.sleep)
    second = lh.poll({}, None, clock=clock, sleep=clock.sleep)
    assert first["called"] and not second["called"]
    assert len(api.calls) == 1


def test_concurrent_duplicates_make_one_call(env: LocalArchive) -> None:
    """Two deliveries of the same minute that both read the archive before either wrote a
    heartbeat: the conditional claim lets exactly one call."""
    from ghostbus_core.archive_layout import claim_key
    from ghostbus_core.feeds import minute_slot

    clock = Clock(MINUTE0 + 2)
    api = Api(clock, ok)
    lh._client = api  # type: ignore[assignment]
    assert env.create_if_absent(claim_key(minute_slot(MINUTE0)), b"the other invocation")
    out = lh.poll({}, None, clock=clock, sleep=clock.sleep)
    assert out == {"called": False, "reason": "claimed", "next_call_at": MINUTE0 + 15}
    assert api.calls == []


def test_backoff_matches_the_long_running_poller(env: LocalArchive) -> None:
    """Same skips as the local loop (test_poller): 0, 1, 3, 7 slots after each failure."""
    api = invoke_each_minute(env, down, minutes=16)
    gaps = [round((b - a) / 60) for (a, _), (b, _) in itertools.pairwise(api.calls)]
    assert gaps == [1, 2, 4, 8]


def test_handover_from_the_laptop_poller_keeps_60s(env: LocalArchive) -> None:
    """The laptop called at second :40 of the minute before; the cloud waits, then realigns."""
    laptop_call = MINUTE0 - 20
    poll_once(
        Feed.VEHICLES,
        client=Api(Clock(laptop_call), ok),
        archive=env,
        publisher=NullPublisher(),
        now_ms=lambda: round(laptop_call * 1000),
    )
    api = invoke_each_minute(env, ok, minutes=3)
    first = api.calls[0][0]
    assert first - laptop_call >= MIN_CALL_SPACING_S
    # :15 this minute is only 35 s after the laptop's call and :40 would be 25 s off the offset,
    # so the cloud poller gives up this minute once and lands on :15 of the next.
    assert first == MINUTE0 + 60 + 15


def test_no_history_calls_at_the_offset(env: LocalArchive) -> None:
    api = invoke_each_minute(env, ok, minutes=1)
    assert [t for t, _ in api.calls] == [MINUTE0 + 15]


def test_call_state_reads_backoff_from_heartbeats(env: LocalArchive) -> None:
    clock = Clock(MINUTE0 + 2)
    api = Api(clock, down)
    lh._client = api  # type: ignore[assignment]
    lh.poll({}, None, clock=clock, sleep=clock.sleep)
    state = lh.call_state(env, clock.now)
    assert state.last_call_s == MINUTE0 + 15 and state.skip_slots == 0  # first failure
    clock.now = MINUTE0 + 62
    lh.poll({}, None, clock=clock, sleep=clock.sleep)
    assert lh.call_state(env, clock.now).skip_slots == 1  # second failure skips one slot
