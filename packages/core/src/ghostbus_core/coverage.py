"""Our own collection gaps, from heartbeats. A gap is any stretch with no collected snapshot.

A feed is "covered" between two collected snapshots no more than ``max_gap`` apart (normal
spacing is 2 to 3 minutes, ADR-0005). Before the first and after the last collected snapshot in
the horizon, everything is a gap: we cannot vouch for what we never looked at.
"""

from __future__ import annotations

import itertools
from collections.abc import Iterable
from datetime import datetime, timedelta


def collection_gaps(
    collected_at: Iterable[datetime], start: datetime, end: datetime, max_gap: timedelta
) -> list[tuple[datetime, datetime]]:
    """Intervals within ``[start, end]`` not covered by snapshots spaced at most ``max_gap``."""
    times = sorted(t for t in collected_at if start <= t <= end)
    if not times:
        return [(start, end)]
    gaps = []
    if times[0] - start > max_gap:
        gaps.append((start, times[0]))
    for a, b in itertools.pairwise(times):
        if b - a > max_gap:
            gaps.append((a, b))
    if end - times[-1] > max_gap:
        gaps.append((times[-1], end))
    return gaps


def overlaps(
    gaps: Iterable[tuple[datetime, datetime]], window_start: datetime, window_end: datetime
) -> bool:
    return any(g0 < window_end and window_start < g1 for g0, g1 in gaps)
