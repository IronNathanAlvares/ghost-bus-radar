"""When to make the next API call: one call per UTC minute, never closer than 60 s, with backoff.

Pure functions; the poller supplies the clock. Retrying inside a minute would be a second call in
that minute, so there are no immediate retries. A failed slot is simply a missed snapshot, which
the heartbeat records, and repeated failures skip whole slots.
"""

from __future__ import annotations

import math
from enum import StrEnum

from ghostbus_core.feeds import MIN_CALL_SPACING_S, minute_slot

#: Never skip more than this many slots in a row (15 minutes of silence at most).
MAX_SKIP_SLOTS = 15
#: A call this late (or less) against its minute's offset keeps 60 s spacing instead of realigning.
REALIGN_TOLERANCE_S = 5.0


class FetchStatus(StrEnum):
    OK = "ok"
    THROTTLED = "throttled"  # HTTP 429
    CLIENT_ERROR = "client_error"  # other 4xx, e.g. a bad or inactive key
    SERVER_ERROR = "server_error"  # 5xx
    NETWORK_ERROR = "network_error"  # timeout, DNS, connection reset
    BAD_PAYLOAD = "bad_payload"  # HTTP 200 but not a decodable FeedMessage

    @classmethod
    def from_http(cls, status_code: int) -> FetchStatus:
        if 200 <= status_code < 300:
            return cls.OK
        if status_code == 429:
            return cls.THROTTLED
        if 400 <= status_code < 500:
            return cls.CLIENT_ERROR
        return cls.SERVER_ERROR


def backoff_slots(
    status: FetchStatus, consecutive_failures: int, retry_after_s: float | None = None
) -> int:
    """How many whole minute slots to skip after a call.

    ``consecutive_failures`` includes the call just made. The first failure skips nothing (the
    next slot is a minute away anyway); then 1, 3, 7, 15 slots. A 429 always skips at least one
    slot and honours ``Retry-After``.
    """
    if status is FetchStatus.OK:
        return 0
    exponential = (1 << max(consecutive_failures - 1, 0)) - 1
    skip = exponential
    if status is FetchStatus.THROTTLED:
        skip = max(skip, 1)
        if retry_after_s is not None and retry_after_s > 0:
            skip = max(skip, math.ceil(retry_after_s / 60) - 1)
    return min(skip, MAX_SKIP_SLOTS)


def next_call_at(
    now_s: float, offset_s: float, last_call_s: float | None, skip_slots: int = 0
) -> float:
    """Epoch seconds of the next call.

    ``offset_s`` is a fixed point inside each minute (so deployments do not all fire on the
    minute boundary, and the Lambda has slack after its scheduler fires). The result is the
    offset point in the first minute after both ``now_s`` and the last call's minute, moved on by
    ``skip_slots`` minutes, and never less than 60 s after the last call:

    - a call that ran up to ``REALIGN_TOLERANCE_S`` late keeps "last call + 60 s" (no lost minute
      for a little jitter);
    - further off the offset (a new offset, or a handover from a poller on another second), one
      minute is given up once to land back on the offset. Before 18 November 2026 the plan was
      ``max(offset point, last + 60)``, which kept calls locked on whatever second they first
      happened at, so a changed offset never took effect.
    """
    if not 0 <= offset_s < 60:
        raise ValueError("offset_s must be in [0, 60)")
    if skip_slots < 0:
        raise ValueError("skip_slots must be >= 0")
    at = minute_slot(now_s) * 60 + offset_s
    if at <= now_s:
        at += 60
    if last_call_s is not None:
        at = max(at, (minute_slot(last_call_s) + 1) * 60 + offset_s)
    at += skip_slots * 60
    if last_call_s is not None and at < last_call_s + MIN_CALL_SPACING_S:
        late = last_call_s + MIN_CALL_SPACING_S - at
        at = last_call_s + MIN_CALL_SPACING_S if late <= REALIGN_TOLERANCE_S else at + 60
    return at


def planned_call_at(
    now_s: float, offset_s: float, last_call_s: float | None, skip_slots: int = 0
) -> float:
    """The next call for a scheduler without memory (the Lambda, fired once a minute).

    It plans exactly as the long-running poller would have right after its last call: the next
    offset point after that call, moved on by ``skip_slots``, at least 60 s later. A plan already
    in the past (the previous invocation never came) becomes the next offset point after
    ``now_s``, still at least 60 s after the last call.
    """
    if last_call_s is None:
        return next_call_at(now_s, offset_s, None)
    planned = next_call_at(last_call_s, offset_s, last_call_s, skip_slots)
    return planned if planned > now_s else next_call_at(now_s, offset_s, last_call_s)
