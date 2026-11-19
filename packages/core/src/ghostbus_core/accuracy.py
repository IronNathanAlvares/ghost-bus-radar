"""How accurate is the countdown? Predicted vs actual arrival at stops (roadmap 1.4,
ADR-0016). Pure.

For one trip that ran: when it actually passed each stop (reconstructed from its positions), what
each archived TripUpdates snapshot predicted for each stop it had not yet reached, and the error
between them, next to the timetable's error at the same moments. Accumulators merge across trips,
partitions and days, so the Spark job only moves small summaries.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from itertools import pairwise
from typing import Any

from ghostbus_core.export import ATTRIBUTION
from ghostbus_core.outcomes import wilson

SCHEMA_VERSION = "v1"
#: Lead time buckets, in minutes: what the board shows when the prediction is made.
LEADS = ((0, 2), (2, 5), (5, 10), (10, 15), (15, 20), (20, 30))
MAX_GAP_S = 240  # interpolate only between fixes at most 4 minutes apart
OFF_ROUTE_M = 150.0  # a fix further than this from the route is ignored
REACHED_M = 1.0  # slack when deciding a fix is at or past a stop
LOOKAHEAD = 15  # segments searched forward from the last match
#: Error histogram: 30-second bins from -10 to +30 minutes (errors beyond go in the end bins).
HIST_MIN_S, HIST_MAX_S, HIST_BIN_S = -600, 1800, 30
HIST_BINS = (HIST_MAX_S - HIST_MIN_S) // HIST_BIN_S
EARTH_M = 6_371_000.0


@dataclass(frozen=True)
class StopTime:
    seq: int
    lat: float
    lon: float
    scheduled: int  # scheduled arrival, epoch seconds


@dataclass(frozen=True)
class Fix:
    t: int  # the position's own timestamp, epoch seconds
    lat: float
    lon: float


@dataclass(frozen=True)
class Update:
    seq: int
    delay: int | None
    time: int | None
    skipped: bool = False


@dataclass(frozen=True)
class Snapshot:
    fetched: int  # epoch seconds
    updates: tuple[Update, ...]


# --- where the bus was, and when it passed each stop ----------------------------------------------


def _xy(lat: float, lon: float, lat0: float) -> tuple[float, float]:
    """Metres on a local flat projection (equirectangular: fine over a bus route)."""
    k = math.pi / 180 * EARTH_M
    return lon * k * math.cos(math.radians(lat0)), lat * k


@dataclass(frozen=True)
class Route:
    stops: tuple[StopTime, ...]
    xy: tuple[tuple[float, float], ...]
    chainage: tuple[float, ...]  # distance along the stop chain at each stop, metres
    lat0: float

    @classmethod
    def of(cls, stops: Sequence[StopTime]) -> Route:
        ordered = tuple(sorted(stops, key=lambda s: s.seq))
        lat0 = ordered[0].lat if ordered else 0.0
        xy = tuple(_xy(s.lat, s.lon, lat0) for s in ordered)
        cum = [0.0]
        for a, b in pairwise(xy):
            cum.append(cum[-1] + math.dist(a, b))
        return cls(ordered, xy, tuple(cum), lat0)

    def project(self, fix: Fix, start: int, end: int) -> tuple[int, float, float] | None:
        """(segment, distance along the route, distance off it) of the nearest point on segments
        start..end-1."""
        p = _xy(fix.lat, fix.lon, self.lat0)
        best: tuple[int, float, float] | None = None
        for i in range(max(start, 0), min(end, len(self.xy) - 1)):
            (ax, ay), (bx, by) = self.xy[i], self.xy[i + 1]
            dx, dy = bx - ax, by - ay
            seg2 = dx * dx + dy * dy
            u = (
                0.0
                if seg2 == 0
                else max(0.0, min(1.0, ((p[0] - ax) * dx + (p[1] - ay) * dy) / seg2))
            )
            off = math.dist(p, (ax + u * dx, ay + u * dy))
            if best is None or off < best[2]:
                best = (i, self.chainage[i] + u * math.sqrt(seg2), off)
        return best


def progress(route: Route, fixes: Iterable[Fix]) -> list[tuple[int, float]]:
    """(time, distance along the route) for the usable fixes, in time order, never going backwards.

    The first fix searches the whole route; later ones search forward from the last match, so a
    route that passes the same street twice is not confused. Fixes off the route, and fixes that
    would move the bus backwards by more than 50 m (GPS noise), are dropped.
    """
    out: list[tuple[int, float]] = []
    seg = None
    seen: set[int] = set()
    for f in sorted(fixes, key=lambda f: f.t):
        if f.t in seen:
            continue
        seen.add(f.t)
        lo, hi = (0, len(route.xy)) if seg is None else (seg - 1, seg + LOOKAHEAD)
        hit = route.project(f, lo, hi)
        if hit is None or hit[2] > OFF_ROUTE_M:
            continue
        if out and hit[1] < out[-1][1] - 50:
            continue
        seg = hit[0]
        out.append((f.t, max(hit[1], out[-1][1]) if out else hit[1]))
    return out


def passages(route: Route, fixes: Iterable[Fix]) -> dict[int, int]:
    """When the bus passed each stop (epoch seconds), by stop sequence. A stop gets a time only if
    the fixes either side of it are at most ``MAX_GAP_S`` apart (constant speed between them).

    The first stop gets none: a bus waits there before leaving, so "when it passed" is ambiguous
    (first seen there, or left?), and its countdown is about departure, not arrival."""
    track = progress(route, fixes)
    out: dict[int, int] = {}
    for stop, d in list(zip(route.stops, route.chainage, strict=True))[1:]:
        for (ta, da), (tb, db) in pairwise(track):
            # "Reached" with a metre's slack: a fix exactly at a stop and the stop itself are
            # measured along the route by different sums, and float rounding put such fixes a
            # hair short of the stop, so a bus dwelling there was timed at departure, not arrival.
            if da - REACHED_M <= d <= db + REACHED_M:
                if tb - ta <= MAX_GAP_S:
                    frac = 0.0 if db <= da else min(1.0, max(0.0, (d - da) / (db - da)))
                    out[stop.seq] = round(ta + frac * (tb - ta))
                break
    return out


# --- what the board said --------------------------------------------------------------------------


def predicted(snap: Snapshot, stop: StopTime) -> int | None:
    """The board's time for ``stop`` in this snapshot: its own update, else the delay of the last
    earlier updated stop carried forward (the API's rule, ADR-0012). A later stop's update says
    nothing about this one."""
    before: Update | None = None
    for u in snap.updates:
        if u.seq == stop.seq:
            if u.skipped:
                return None
            if u.time is not None:
                return u.time
            if u.delay is not None:
                return stop.scheduled + u.delay
        elif (
            u.seq < stop.seq
            and u.delay is not None
            and not u.skipped
            and (before is None or before.seq < u.seq)
        ):
            before = u
    return (
        stop.scheduled + before.delay if before is not None and before.delay is not None else None
    )


def lead_bucket(lead_s: float) -> str | None:
    for lo, hi in LEADS:
        if lo * 60 <= lead_s < hi * 60:
            return f"{lo}-{hi}"
    return None


@dataclass(frozen=True)
class Error:
    bucket: str
    lead_s: int
    error_s: int  # actual - predicted
    timetable_s: int  # actual - scheduled, at the same moment


def trip_errors(
    stops: Sequence[StopTime], snapshots: Iterable[Snapshot], fixes: Iterable[Fix]
) -> tuple[list[Error], int, int]:
    """Every (snapshot, stop not yet passed) prediction of one trip, scored. Also returns how many
    of its stops got a passage time, and how many could have (all but the first)."""
    route = Route.of(stops)
    passed = passages(route, fixes)
    out = []
    for snap in snapshots:
        for stop in route.stops:
            actual = passed.get(stop.seq)
            if actual is None or actual <= snap.fetched:
                continue
            pred = predicted(snap, stop)
            if pred is None:
                continue
            lead = pred - snap.fetched
            bucket = lead_bucket(lead)
            if bucket is None:
                continue
            out.append(Error(bucket, lead, actual - pred, actual - stop.scheduled))
    return out, len(passed), max(len(route.stops) - 1, 0)


# --- summaries that merge -------------------------------------------------------------------------


def _bin(err_s: int) -> int:
    return max(0, min(HIST_BINS - 1, (err_s - HIST_MIN_S) // HIST_BIN_S))


@dataclass
class Acc:
    """Errors of one group (operator x lead bucket). Merges by addition."""

    n: int = 0
    trips: int = 0
    sum_err: int = 0
    sum_abs: int = 0
    within_60: int = 0
    within_120: int = 0
    tt_sum_abs: int = 0
    tt_within_120: int = 0
    hist: list[int] = field(default_factory=lambda: [0] * HIST_BINS)
    tt_hist: list[int] = field(default_factory=lambda: [0] * HIST_BINS)

    def add(self, e: Error) -> None:
        self.n += 1
        self.sum_err += e.error_s
        self.sum_abs += abs(e.error_s)
        self.within_60 += abs(e.error_s) <= 60
        self.within_120 += abs(e.error_s) <= 120
        self.tt_sum_abs += abs(e.timetable_s)
        self.tt_within_120 += abs(e.timetable_s) <= 120
        self.hist[_bin(e.error_s)] += 1
        self.tt_hist[_bin(e.timetable_s)] += 1

    def merge(self, o: Acc) -> Acc:
        return Acc(
            self.n + o.n,
            self.trips + o.trips,
            self.sum_err + o.sum_err,
            self.sum_abs + o.sum_abs,
            self.within_60 + o.within_60,
            self.within_120 + o.within_120,
            self.tt_sum_abs + o.tt_sum_abs,
            self.tt_within_120 + o.tt_within_120,
            [a + b for a, b in zip(self.hist, o.hist, strict=True)],
            [a + b for a, b in zip(self.tt_hist, o.tt_hist, strict=True)],
        )

    def to_row(self) -> dict[str, Any]:
        return {k: getattr(self, k) for k in self.__dataclass_fields__}

    @classmethod
    def from_row(cls, r: Mapping[str, Any]) -> Acc:
        return cls(
            **{  # type: ignore[arg-type]
                k: (list(r[k]) if k.endswith("hist") else int(r[k]))
                for k in cls.__dataclass_fields__
            }
        )


def accumulate(errors: Iterable[Error], agency: str) -> dict[tuple[str, str], Acc]:
    """One trip's errors as accumulators by (operator, lead bucket); each counts the trip once."""
    out: dict[tuple[str, str], Acc] = {}
    for e in errors:
        key = (agency, e.bucket)
        if key not in out:
            out[key] = Acc(trips=1)
        out[key].add(e)
    return out


def quantile(hist: Sequence[int], q: float) -> float | None:
    """The q-quantile of a histogram, in minutes (middle of the bin it falls in)."""
    total = sum(hist)
    if not total:
        return None
    target, seen = q * total, 0
    for i, c in enumerate(hist):
        seen += c
        if seen >= target:
            return (HIST_MIN_S + (i + 0.5) * HIST_BIN_S) / 60
    return HIST_MAX_S / 60


def _r(x: float | None, d: int = 2) -> float | None:
    return None if x is None else round(x, d)


def summary(a: Acc) -> dict[str, Any]:
    """The published numbers for one group. Shares' intervals use the trip count, not the
    prediction count: a late bus is late at every stop, so predictions aren't independent."""
    if not a.n:
        return {"predictions": 0, "trips": a.trips}

    def share(k: int) -> dict[str, Any]:
        lo, hi = wilson(round(k / a.n * a.trips), a.trips)
        return {"share": _r(k / a.n, 4), "lo": _r(lo, 4), "hi": _r(hi, 4)}

    return {
        "predictions": a.n,
        "trips": a.trips,
        "median_min": _r(quantile(a.hist, 0.5)),
        "p10_min": _r(quantile(a.hist, 0.1)),
        "p90_min": _r(quantile(a.hist, 0.9)),
        "mean_min": _r(a.sum_err / a.n / 60),
        "mae_min": _r(a.sum_abs / a.n / 60),
        "within_1_min": share(a.within_60),
        "within_2_min": share(a.within_120),
        "timetable": {
            "median_min": _r(quantile(a.tt_hist, 0.5)),
            "p10_min": _r(quantile(a.tt_hist, 0.1)),
            "p90_min": _r(quantile(a.tt_hist, 0.9)),
            "mae_min": _r(a.tt_sum_abs / a.n / 60),
            "within_2_min": share(a.tt_within_120),
        },
    }


def accuracy_doc(
    groups: Mapping[tuple[str, str], Acc],
    days: Sequence[str],
    coverage: tuple[int, int],
    generated_at: str,
) -> dict[str, Any]:
    """``data/v1/accuracy.json``: by lead bucket over all operators, and by operator."""
    buckets = [f"{lo}-{hi}" for lo, hi in LEADS]
    agencies = sorted({a for a, _ in groups})

    def total(bucket: str, agency: str | None = None) -> Acc:
        acc = Acc()
        for (a, b), x in groups.items():
            if b == bucket and (agency is None or a == agency):
                acc = acc.merge(x)
        return acc

    timed, stops = coverage
    return {
        "schema": f"ghostbus.accuracy.{SCHEMA_VERSION}",
        "attribution": ATTRIBUTION,
        "generated_at": generated_at,
        "days": list(days),
        "stops_timed_share": _r(timed / stops, 4) if stops else None,
        "error_is": "actual arrival minus predicted, in minutes; positive means later than shown",
        "by_lead": [{"lead_min": b} | summary(total(b)) for b in buckets],
        "by_operator": [
            {"agency": a, "by_lead": [{"lead_min": b} | summary(total(b, a)) for b in buckets]}
            for a in agencies
        ],
    }
