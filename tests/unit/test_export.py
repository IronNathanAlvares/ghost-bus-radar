"""The public export (data/v1): publishing rules, conservative ranking, determinism, and every
document valid against its JSON Schema."""

from __future__ import annotations

import json
from datetime import date
from typing import Any

import jsonschema
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from ghostbus_core import schemas
from ghostbus_core.export import (
    MIN_RANKED,
    Cell,
    Stop,
    build,
    dumps,
    league_doc,
    publishable,
    route_doc,
    slug,
)

D18, D19, D20 = date(2026, 11, 18), date(2026, 11, 19), date(2026, 11, 20)
QUALITY = {D18: "fail", D19: "ok", D20: "warn"}
STOPS = [Stop("1358", "Merrion Square", 53.3398, -6.2489, ("46A", "39"))]


def cells() -> list[Cell]:
    return [
        # 46A: 40 trips a day, 2 untracked; the failed day 18 Nov is terrible on purpose.
        Cell(D18, "Dublin Bus", "46A", 8, n=40, ran=10, ghost=30),
        Cell(D19, "Dublin Bus", "46A", 8, n=40, ran=38, ghost=2),
        Cell(D20, "Dublin Bus", "46A", 17, n=40, ran=37, ghost=1, phantom=1, vanished=1),
        # 39: worse point estimate (20%) but tiny sample: must not be ranked.
        Cell(D19, "Dublin Bus", "39", 9, n=10, ran=8, ghost=2),
        # 101: 30 trips, 6 untracked (20%): ranked, and above 46A.
        Cell(D19, "Bus Éireann", "101", 7, n=30, ran=24, ghost=5, phantom=1),
    ]


def docs() -> dict[str, dict[str, Any]]:
    return build(cells(), QUALITY, STOPS, D20, "2026-11-21T05:00:00Z", "rules5-test", "ab" * 32)


def test_failed_days_are_listed_but_never_counted() -> None:
    d = docs()
    assert d["days/2026-11-18.json"]["published"] is False
    assert "trips_in_rates" not in d["days/2026-11-18.json"]
    route = d["routes/dublin-bus-46a.json"]
    assert route["total"]["trips_in_rates"] == 80  # 19 + 20 Nov only, not the failed 18th
    assert [x["date"] for x in route["daily"]] == ["2026-11-19", "2026-11-20"]
    assert D18 not in publishable(QUALITY) and publishable({D18: "missing"}) == {D18}


def test_ranking_needs_enough_trips_and_uses_the_lower_bound() -> None:
    league = league_doc(cells(), QUALITY, D20, 7)
    ranked = [r["slug"] for r in league["routes"]]
    assert ranked == ["bus-eireann-101", "dublin-bus-46a"]
    assert [r["slug"] for r in league["not_ranked"]] == ["dublin-bus-39"]
    assert all(r["trips_in_rates"] >= MIN_RANKED for r in league["routes"])
    assert [r["rank"] for r in league["routes"]] == [1, 2]
    assert league["days_published"] == ["2026-11-19", "2026-11-20"]


def test_heatmap_by_day_type_and_hour() -> None:
    doc = route_doc("Dublin Bus", "46A", cells(), QUALITY, D20)
    cellsets = {(x["day_type"], x["hour"]): x["trips_in_rates"] for x in doc["by_hour"]}
    assert cellsets == {("weekday", 8): 40, ("weekday", 17): 40}


def test_every_document_matches_its_schema_and_carries_attribution() -> None:
    for path, doc in docs().items():
        name = next(v for k, v in schemas.EXPORT.items() if path.startswith(k))
        jsonschema.validate(doc, schemas.load(name))
        assert doc["attribution"]["licence"] == "CC BY 4.0"


def test_same_data_same_bytes() -> None:
    a = {k: dumps(v) for k, v in docs().items()}
    b = {k: dumps(v) for k, v in docs().items()}
    assert a == b
    shuffled = list(reversed(cells()))
    c = build(shuffled, QUALITY, STOPS, D20, "2026-11-21T05:00:00Z", "rules5-test", "ab" * 32)
    assert {k: dumps(v) for k, v in c.items()} == a  # input order never changes a file


def test_meta_lists_every_file_and_the_latest_published_day() -> None:
    d = docs()
    meta = d["meta.json"]
    assert meta["latest_published_day"] == "2026-11-20"
    assert set(meta["files"]) == set(d) - {"meta.json"}
    assert meta["archive_chain_head"] == "ab" * 32


@pytest.mark.parametrize(
    ("agency", "route", "expected"),
    [("Dublin Bus", "46A", "dublin-bus-46a"), ("Bus Éireann", "X1/2", "bus-eireann-x1-2")],
)
def test_slug(agency: str, route: str, expected: str) -> None:
    assert slug(agency, route) == expected


@settings(max_examples=200)
@given(
    st.lists(
        st.tuples(st.sampled_from([D18, D19, D20]), st.integers(1, 60), st.integers(0, 60)),
        min_size=1,
        max_size=20,
    )
)
def test_no_rate_ever_includes_a_failed_day(rows: list[tuple[date, int, int]]) -> None:
    cs = [Cell(d, "Op", "R", 8, n=n, ran=n - min(g, n), ghost=min(g, n)) for d, n, g in rows]
    doc = route_doc("Op", "R", cs, QUALITY, D20)
    expected = sum(c.n for c in cs if c.service_date != D18)
    assert doc["total"]["trips_in_rates"] == expected
    for r in doc["daily"]:
        assert r["date"] != "2026-11-18"
    json.dumps(doc)  # serialisable
