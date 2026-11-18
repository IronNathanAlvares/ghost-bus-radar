"""What happened to a scheduled trip: the outcome classifier. Pure, so every path shares it.

The live stream (provisional outcomes), the nightly reconcile and the backfill all call
:func:`classify` on the same :class:`Evidence`, so "same logic live and in backfill" is enforced
by construction. Thresholds live in :class:`Params`; the sensitivity analysis varies them.

Full definitions and their limits: ``docs/03-ghost-definition.md``.

Outcomes, in the order the rules are checked:

=================  ===========================================================================
OUT_OF_SCOPE       Rail or Luas: NTA publishes no vehicle positions for them, so absence of a
                   vehicle proves nothing. Never counted in any rate.
CANCELLED_EARLY    Marked CANCELED at least ``cancel_lead_min`` before scheduled departure.
CANCELLED_LATE     Marked CANCELED later than that, and we were collecting throughout.
RAN                Tracked through at least ``ran_progress`` of the route's stops.
VANISHED           Tracked on the route (reached at least one stop), then lost before
                   reaching ``ran_progress``.
PHANTOM_LIVE       Not cancelled and never reached a stop, but a vehicle was assigned (in
                   TripUpdates, or reporting positions off the route): the board showed a live
                   countdown to a bus that never came.
GHOST              Never tracked, not cancelled, no vehicle ever assigned.
NO_DATA            The evidence could have been hidden by a gap in our own collection.
                   Excluded from every rate. Our downtime is never counted as a ghost.
=================  ===========================================================================

A bus tracked to the end stands even across a collection gap. Everything else needs the
evidence window covered, including cancellations: a gap could hide that a cancelled trip ran.

That asymmetry is right for one trip and wrong for a rate: across a gap, buses that ran stay
RAN while buses that did not become NO_DATA, so a rate over every decided trip overstates
service. Rates therefore use :func:`in_rates` trips only, whose whole window was collected.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum


class Outcome(StrEnum):
    OUT_OF_SCOPE = "OUT_OF_SCOPE"
    CANCELLED_EARLY = "CANCELLED_EARLY"
    CANCELLED_LATE = "CANCELLED_LATE"
    RAN = "RAN"
    VANISHED = "VANISHED"
    PHANTOM_LIVE = "PHANTOM_LIVE"
    GHOST = "GHOST"
    NO_DATA = "NO_DATA"


#: Outcomes a passenger experiences as "the bus did not come" without warning.
UNTRACKED = frozenset({Outcome.GHOST, Outcome.PHANTOM_LIVE})
#: Outcomes that count towards rates (denominator).
RATED = frozenset(set(Outcome) - {Outcome.OUT_OF_SCOPE, Outcome.NO_DATA})
#: Route types with vehicle positions in the NTA feed (3 = bus).
TRACKED_ROUTE_TYPES = frozenset({"3"})


#: Bump on any change to the rules in :func:`classify`, so stored outcomes from older rules
#: are recognisably stale and get re-decided. 3: cancellations need vehicle coverage.
#: 4: evidence counts only data fetched by the trip's close time.
RULES_VERSION = 4


@dataclass(frozen=True)
class Params:
    """Thresholds. Changing any of them changes ``version``, stored with every outcome."""

    ran_progress: float = 0.6  # share of stops reached to count as RAN
    cancel_lead_min: int = 15  # minimum notice for a cancellation to be "early"
    stop_radius_m: int = 250  # a position within this distance "reached" a stop
    max_collection_gap_min: int = 5  # normal spacing per feed is 2-3 min (ADR-0005)
    window_before_min: int = 30  # coverage needed before scheduled start
    window_after_min: int = 30  # coverage needed after scheduled end
    close_after_min: int = 60  # decide this long after scheduled end (buses run late)

    @property
    def version(self) -> str:
        return (
            f"rules{RULES_VERSION}-p{self.ran_progress:g}-c{self.cancel_lead_min}-r{self.stop_radius_m}"
            f"-g{self.max_collection_gap_min}-w{self.window_before_min}.{self.window_after_min}"
            f"-x{self.close_after_min}"
        )


DEFAULT = Params()


@dataclass(frozen=True)
class Evidence:
    """Everything observed about one scheduled trip instance."""

    route_type: str
    scheduled_start: datetime
    #: First time a TripUpdate said CANCELED (fetch time), if ever.
    cancelled_at: datetime | None
    #: Snapshots in which the trip appeared in TripUpdates.
    tu_snapshots: int
    #: Any TripUpdate named a vehicle for the trip.
    tu_vehicle: bool
    #: Distinct vehicle positions observed for the trip.
    vp_positions: int
    #: Highest share of the trip's stops reached by an observed position (0..1), if tracked.
    progress: float | None
    #: A collection gap (TripUpdates feed) overlapped the window from start - 30 min to the
    #: first cancellation (or to end + 30 min if never cancelled).
    tu_gap: bool
    #: A collection gap (Vehicles feed) overlapped start - 30 min .. end + 30 min.
    vp_gap: bool


def classify(e: Evidence, p: Params = DEFAULT) -> tuple[Outcome, str]:
    """The outcome and a one-line reason a person can check."""
    if e.route_type not in TRACKED_ROUTE_TYPES:
        return Outcome.OUT_OF_SCOPE, f"route_type {e.route_type} has no vehicle positions"

    progress = e.progress or 0.0
    if e.cancelled_at is not None:
        if progress >= p.ran_progress:
            return Outcome.RAN, f"ran ({progress:.0%} of stops) despite a cancellation notice"
        notice = (e.scheduled_start - e.cancelled_at).total_seconds() / 60
        if e.vp_gap:
            # Found by the outage demo: a trip cancelled early that then ran anyway looked
            # "cancelled" once a gap hid its positions. A gap leaves "did it run?" open.
            return Outcome.NO_DATA, "cancelled, but a collection gap hides whether it ran anyway"
        if notice >= p.cancel_lead_min:
            return Outcome.CANCELLED_EARLY, f"cancelled {notice:.0f} min before departure"
        if e.tu_gap:
            return Outcome.NO_DATA, "cancelled, but a collection gap hides when it was announced"
        return Outcome.CANCELLED_LATE, f"cancelled only {notice:.0f} min before departure"

    if progress >= p.ran_progress:
        return Outcome.RAN, f"tracked through {progress:.0%} of stops"
    if e.vp_positions > 0 and progress > 0:
        # Either feed matters: a mid-route cancellation published during a TripUpdates gap
        # would also explain a bus disappearing.
        if e.vp_gap or e.tu_gap:
            return Outcome.NO_DATA, f"tracked to {progress:.0%}, then a collection gap"
        return Outcome.VANISHED, f"tracked to {progress:.0%} of stops, then lost"
    if e.vp_gap or e.tu_gap:
        return Outcome.NO_DATA, "never seen on the route, but our collection had a gap"
    if e.vp_positions > 0:
        # Seen in real data: a bus assigned to the trip reporting every two minutes from one
        # spot 1 km from the first stop, for the whole trip. The board showed it live; it never
        # served a stop.
        return Outcome.PHANTOM_LIVE, "a vehicle reported positions but never reached a stop"
    if e.tu_vehicle:
        return Outcome.PHANTOM_LIVE, "a vehicle was assigned in TripUpdates but never tracked"
    if e.tu_snapshots > 0:
        return Outcome.GHOST, "listed in TripUpdates without a vehicle; never tracked"
    return Outcome.GHOST, "never tracked and absent from TripUpdates"


def in_rates(e: Evidence) -> bool:
    """Whether a trip belongs in published rates: a bus trip whose whole evidence window was
    collected, so every outcome had the same chance of being observed (no survivorship from
    RAN surviving gaps). Such a trip is never NO_DATA."""
    return e.route_type in TRACKED_ROUTE_TYPES and not (e.tu_gap or e.vp_gap)


def close_time(scheduled_end: datetime, p: Params = DEFAULT) -> datetime:
    """When a trip's outcome can be decided: everything that could be observed has arrived."""
    return scheduled_end + timedelta(minutes=p.close_after_min)


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """Wilson score interval for k successes in n trials (95% by default)."""
    if n == 0:
        return (0.0, 1.0)
    phat = k / n
    denom = 1 + z * z / n
    centre = (phat + z * z / (2 * n)) / denom
    half = z * math.sqrt(phat * (1 - phat) / n + z * z / (4 * n * n)) / denom
    return (max(0.0, centre - half), min(1.0, centre + half))
