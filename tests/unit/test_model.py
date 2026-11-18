"""heuristic-v2 (ADR-0015): shrinkage strengths estimated from the data, thin routes pulled toward
the network, "cancelled" from each operator's record, a network answer for routes without
history, the published file round-trips and matches its schema, and the API uses it."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta

import jsonschema
import pytest
from hypothesis import given
from hypothesis import strategies as st

from ghostbus_core import schemas
from ghostbus_core.departures import Departure, Trip, departures_doc, history_from_route_doc
from ghostbus_core.export import Cell, route_doc
from ghostbus_core.model import KAPPA_MAX, KAPPA_MIN, NAME, Model, fit, kappa
from tests.unit.test_departures import DEPS, HISTORY, NOW, STOP, feeds

MON = date(2026, 11, 16)


def week(route: str, n: int, ran: int, agency: str = "Dublin Bus", hour: int = 8) -> list[Cell]:
    return [
        Cell(MON + timedelta(days=i), agency, route, hour, n=n, ran=ran, ghost=n - ran)
        for i in range(5)
    ]


def departure(route: str = "46A", agency: str = "Dublin Bus", hour: int = 8) -> Departure:
    start = int(datetime(2026, 11, 16, hour, tzinfo=UTC).timestamp())  # November: Dublin = UTC
    trip = Trip("t", "r", "0", "20261116", "", start, agency, route, "", "x")
    return Departure("1358", start + 600, 3, trip)


def test_kappa_follows_the_real_spread() -> None:
    same = [(90, 100, 0.9)] * 10  # every unit exactly at its centre: all differences are noise
    assert kappa(same) == KAPPA_MAX
    spread = [(50, 100, 0.75), (100, 100, 0.75)] * 5  # 50% and 100% around 75%: a real spread
    assert KAPPA_MIN <= kappa(spread) < 10
    assert kappa([(1, 2, 0.5), (2, 2, 0.5)]) == KAPPA_MAX  # too few units to tell: shrink hard


def test_a_thin_route_is_pulled_toward_the_network() -> None:
    cells = [*week("46A", 40, 36), *week("39", 40, 30), *week("7", 40, 38), *week("1", 1, 1)]
    model = fit(cells)
    hist = {
        r: history_from_route_doc(
            route_doc(
                "Dublin Bus",
                r,
                [c for c in cells if c.route == r],
                {c.service_date: "ok" for c in cells},
                MON,
            )
        )
        for r in ("1", "39")
    }
    p_thin, basis = model.probability(hist["1"], departure("1"), "scheduled", 0, False)
    p_bad, _ = model.probability(hist["39"], departure("39"), "scheduled", 0, False)
    assert p_thin is not None and p_bad is not None
    assert p_thin < 0.99  # 5 of 5 ran, but 5 trips is not proof of 100%
    assert p_bad < p_thin  # a route with plenty of evidence of trouble stays lower
    assert basis["level"] == "route x day type x hour"


def test_cancelled_uses_the_operators_record() -> None:
    cells = [
        Cell(MON, "Bus Éireann", "101", 8, n=50, ran=45, notice=20, notice_ran=12),
        Cell(MON, "Dublin Bus", "46A", 8, n=50, ran=45, notice=20, notice_ran=0),
    ]
    model = fit(cells)
    p_be, basis = model.cancelled("Bus Éireann")
    p_db, _ = model.cancelled("Dublin Bus")
    p_new, _ = model.cancelled("Go-Ahead Ireland")  # no record: the network figure
    assert p_be is not None and p_db is not None and p_new is not None
    assert p_be > p_new > p_db > 0
    assert p_new == pytest.approx(12 / 40, abs=0.01)
    assert basis["operator_cancellations"] == 20
    assert model.probability(None, departure(), "skips_stop", 0, True)[0] == 0.0


def test_no_history_means_the_network_rate_not_silence() -> None:
    model = fit(week("46A", 40, 36))
    p, basis = model.probability(None, departure("unknown"), "scheduled", 0, False)
    assert p == pytest.approx(0.9) and basis["level"] == "network"


def test_the_published_file_round_trips_and_matches_its_schema() -> None:
    cells = [
        *week("46A", 40, 36),
        *week("39", 40, 30),
        *week("7", 40, 38),
        Cell(MON, "Bus Éireann", "101", 8, n=10, ran=9, notice=3, notice_ran=2),
    ]
    doc = fit(cells).to_doc()
    jsonschema.validate(doc, schemas.load(schemas.EXPORT["model.json"]))
    again = Model.from_doc(json.loads(json.dumps(doc)))
    assert again.to_doc() == doc
    assert fit(list(reversed(cells))).to_doc() == doc  # order never matters
    assert doc["model"] == NAME and set(doc["priors"]) == {
        "scheduled",
        "scheduled_near",
        "assigned",
        "tracked",
    }


def test_departures_answer_with_the_model_when_given_one() -> None:
    from ghostbus_core.departures import live_view

    model = fit([*week("46A", 40, 36), *week("39", 40, 30), *week("7", 40, 38)])
    doc = departures_doc(STOP, DEPS, live_view(*feeds()), HISTORY, NOW, model=model)
    jsonschema.validate(doc, schemas.load(schemas.DEPARTURES))
    assert doc["model"] == NAME
    rows = {r["trip_id"]: r for r in doc["departures"]}
    assert rows["t5"]["probability"] is not None  # route 39 had no history: network answer now
    assert rows["t3"]["probability"] == 0.0  # cancelled, and no operator record of reversals
    assert rows["t1"]["probability"] > rows["t4"]["probability"]  # tracked above scheduled


@given(
    st.lists(
        st.tuples(st.integers(1, 60), st.integers(0, 60), st.integers(0, 60), st.integers(0, 20)),
        min_size=1,
        max_size=30,
    ),
    st.sampled_from(["scheduled", "assigned", "tracked", "no_live_data", "cancelled"]),
    st.booleans(),
)
def test_always_a_probability(
    rows: list[tuple[int, int, int, int]], status: str, fresh: bool
) -> None:
    cells = []
    for i, (n, ran, ghost, notice) in enumerate(rows):
        ran = min(ran, n)
        ghost = min(ghost, n - ran)
        cells.append(
            Cell(
                MON,
                "Dublin Bus",
                f"r{i % 4}",
                6 + i % 12,
                n=n,
                ran=ran,
                ghost=ghost,
                notice=notice,
                notice_ran=min(notice, ran),
            )
        )
    model = fit(cells)
    hist = history_from_route_doc(
        route_doc("Dublin Bus", "r0", [c for c in cells if c.route == "r0"], {MON: "ok"}, MON)
    )
    p, _ = model.probability(hist, departure("r0"), status, 0, fresh)
    assert p is None or 0.0 <= p <= 1.0
