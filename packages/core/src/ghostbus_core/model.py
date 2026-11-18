"""heuristic-v2: the "will my bus come?" probability, fixed by what the backtest found (ADR-0015).

The calibration backtest (ADR-0014, measurement 08) showed heuristic-v1 was badly calibrated in
two ways: thin routes got extreme guesses, and every "cancelled" bus got 0% although many still
ran. v2 keeps v1's idea (the share of similar trips that ran, among the outcomes the live status
still allows) and changes how the evidence is pooled:

- **Hierarchical empirical Bayes.** An hour's rate is shrunk toward its route's rate, and the
  route's toward the network's (v1 stopped at the route, so a route with 3 trips, all run, said
  100%). How hard to shrink is *estimated* from the data, separately at each level, by the method
  of moments for a beta-binomial (Efron and Morris): if routes really differ, little shrinkage;
  if their differences are mostly noise, a lot. Bounded to [5, 2000] trips because the estimate is
  unstable with few units.
- **"Cancelled" is not 0%.** The chance a bus shown as cancelled still runs, per operator, shrunk
  toward the network figure with a prior worth 10 cancellations. (Measured: Bus Éireann 41 of 70.)
- **No history is not no answer.** A route without history gets the network rate, labelled so.

Everything the API needs beyond one route's file is published nightly as ``data/v1/model.json``,
fitted on the published (quality-passed) days; the backtest fits the same function on the days
before each test day.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any

from ghostbus_core.export import ATTRIBUTION, Cell, day_type
from ghostbus_core.outcomes import DEFAULT
from ghostbus_core.timetable import LOCAL_TZ

NAME = "heuristic-v2"
SCHEMA_VERSION = "v1"
KAPPA_MIN = 5.0
KAPPA_MAX = 2000.0
CANCEL_PRIOR = 10.0
CANCEL_LEAD_S = DEFAULT.cancel_lead_min * 60
#: The live status decides which outcomes are still possible (as in v1): four cases.
CASES = ("scheduled", "scheduled_near", "assigned", "tracked")
DIGITS = 2


@dataclass(frozen=True)
class Counts:
    """Trips and the outcomes that matter for the probability."""

    n: int = 0
    ran: int = 0
    ghost: int = 0
    phantom: int = 0
    early: int = 0

    def __add__(self, o: Counts) -> Counts:
        return Counts(
            self.n + o.n,
            self.ran + o.ran,
            self.ghost + o.ghost,
            self.phantom + o.phantom,
            self.early + o.early,
        )


def _of(c: Cell) -> Counts:
    return Counts(c.n, c.ran, c.ghost, c.phantom, c.cancelled_early)


def case(status: str, near: bool) -> str:
    if status in ("tracked", "assigned"):
        return status
    return "scheduled_near" if status == "scheduled" and near else "scheduled"


def allowed(t: Any, which: str) -> tuple[int, int]:
    """(ran, trips) among the outcomes still possible in this case. ``t`` has n, ran, ghost,
    phantom, early (a ``Counts`` or a ``departures.Tally``)."""
    out = 0
    if which in ("tracked", "assigned"):
        out = t.ghost + t.early + (t.phantom if which == "tracked" else 0)
    elif which == "scheduled_near":
        out = t.early
    return t.ran, max(t.n - out, 0)


def kappa(units: Iterable[tuple[int, int, float]]) -> float:
    """Method-of-moments prior strength (in trips) for rates k/n around centres m.

    The spread of the observed rates around their centres, minus the spread binomial noise alone
    would give, is the real spread tau^2; a beta prior with that spread has strength
    m(1-m)/tau^2 - 1. No real spread means shrink hard (the maximum).
    """
    rows = [(k, n, m) for k, n, m in units if n > 0]
    if len(rows) < 3:
        return KAPPA_MAX
    total = sum(n for _, n, _ in rows)
    spread = sum(n * (k / n - m) ** 2 for k, n, m in rows) / total
    noise = sum(m * (1 - m) for _, _, m in rows) / total
    mbar = sum(n * m for _, n, m in rows) / total
    tau2 = spread - noise
    if tau2 <= 0:
        return KAPPA_MAX
    return max(KAPPA_MIN, min(KAPPA_MAX, mbar * (1 - mbar) / tau2 - 1))


@dataclass(frozen=True)
class Model:
    network: Counts
    #: case -> (route prior, hour prior), in trips
    priors: Mapping[str, tuple[float, float]]
    cancel_network: tuple[int, int]  # (notices, ran anyway)
    cancel_by_agency: Mapping[str, tuple[int, int]]
    days: tuple[str, ...] = ()
    name: str = field(default=NAME)

    # --- the probability --------------------------------------------------------------------------

    def cancelled(self, agency: str) -> tuple[float | None, dict[str, Any]]:
        net_n, net_k = self.cancel_network
        if not net_n:
            return 0.0, {"reason": "cancelled"}
        n, k = self.cancel_by_agency.get(agency, (0, 0))
        p = (k + CANCEL_PRIOR * net_k / net_n) / (n + CANCEL_PRIOR)
        return round(p, DIGITS), {
            "reason": "cancelled",
            "operator_cancellations": n,
            "operator_cancelled_but_ran": k,
        }

    def probability(
        self, history: Any, d: Any, status: str, now: int, live_fresh: bool
    ) -> tuple[float | None, dict[str, Any]]:
        """Same signature as ``departures.probability``; ``history`` is a ``RouteHistory``."""
        if status == "skips_stop":
            return 0.0, {"reason": "skips stop"}
        if status == "cancelled":
            return self.cancelled(d.trip.agency)
        which = case(status, live_fresh and d.at - now < CANCEL_LEAD_S)
        kn, nn = allowed(self.network, which)
        if not nn:
            return None, {"reason": "no published history yet"}
        network = kn / nn
        if history is None or history.total.n == 0:
            return round(network, DIGITS), {"level": "network", "network_rate": round(network, 4)}
        k_route, k_hour = self.priors.get(which, (KAPPA_MAX, KAPPA_MAX))
        k, n = allowed(history.total, which)
        route = (k + k_route * network) / (n + k_route)
        start = datetime.fromtimestamp(d.trip.start_at, UTC).astimezone(LOCAL_TZ)
        sd = d.trip.start_date
        key = (day_type(date(int(sd[:4]), int(sd[4:6]), int(sd[6:]))), start.hour)
        cell = history.cells.get(key)
        kc, nc = allowed(cell, which) if cell is not None else (0, 0)
        p = (kc + k_hour * route) / (nc + k_hour)
        return round(p, DIGITS), {
            "level": "route x day type x hour",
            "day_type": key[0],
            "hour": key[1],
            "trips_this_hour": nc,
            "trips_on_route": n,
            "route_rate": round(route, 4),
            "network_rate": round(network, 4),
        }

    # --- the published file -------------------------------------------------------------------

    def to_doc(self) -> dict[str, Any]:
        net = self.network
        return {
            "schema": f"ghostbus.model.{SCHEMA_VERSION}",
            "attribution": ATTRIBUTION,
            "model": self.name,
            "days": list(self.days),
            "network": {
                "trips": net.n,
                "ran": net.ran,
                "ghost": net.ghost,
                "phantom": net.phantom,
                "cancelled_early": net.early,
            },
            "priors": {
                c: {"route_trips": round(r, 2), "hour_trips": round(h, 2)}
                for c, (r, h) in sorted(self.priors.items())
            },
            "cancelled": {
                "prior_trips": CANCEL_PRIOR,
                "network": {"notices": self.cancel_network[0], "ran": self.cancel_network[1]},
                "by_operator": [
                    {"agency": a, "notices": n, "ran": k}
                    for a, (n, k) in sorted(self.cancel_by_agency.items())
                ],
            },
        }

    @classmethod
    def from_doc(cls, doc: Mapping[str, Any]) -> Model:
        net = doc["network"]
        return cls(
            Counts(net["trips"], net["ran"], net["ghost"], net["phantom"], net["cancelled_early"]),
            {c: (v["route_trips"], v["hour_trips"]) for c, v in doc["priors"].items()},
            (doc["cancelled"]["network"]["notices"], doc["cancelled"]["network"]["ran"]),
            {r["agency"]: (r["notices"], r["ran"]) for r in doc["cancelled"]["by_operator"]},
            tuple(doc.get("days", ())),
            doc.get("model", NAME),
        )


def fit(cells: Sequence[Cell]) -> Model:
    """The model from fully collected trips: network counts, the prior strengths for each case,
    and the cancellation figures. Pure and deterministic (cells in any order give the same)."""
    network = Counts()
    routes: dict[tuple[str, str], Counts] = defaultdict(Counts)
    hours: dict[tuple[str, str, str, int], Counts] = defaultdict(Counts)
    notices: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    for c in cells:
        x = _of(c)
        network = network + x
        routes[(c.agency, c.route)] = routes[(c.agency, c.route)] + x
        key = (c.agency, c.route, day_type(c.service_date), c.hour)
        hours[key] = hours[key] + x
        notices[c.agency][0] += c.notice
        notices[c.agency][1] += c.notice_ran
    priors: dict[str, tuple[float, float]] = {}
    for which in CASES:
        kn, nn = allowed(network, which)
        mu = kn / nn if nn else 0.0
        k_route = kappa((*allowed(t, which), mu) for t in routes.values())
        centre = {
            r: (allowed(t, which)[0] + k_route * mu) / (allowed(t, which)[1] + k_route)
            for r, t in routes.items()
        }
        k_hour = kappa((*allowed(t, which), centre[(h[0], h[1])]) for h, t in hours.items())
        priors[which] = (k_route, k_hour)
    by_agency = {a: (v[0], v[1]) for a, v in notices.items() if v[0]}
    return Model(
        network,
        priors,
        (sum(v[0] for v in by_agency.values()), sum(v[1] for v in by_agency.values())),
        by_agency,
        tuple(sorted({c.service_date.isoformat() for c in cells})),
    )
