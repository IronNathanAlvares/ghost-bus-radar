"""'Will my bus come?' (ADR-0012): the stop index, live matching, expected times, statuses and the
probability, all on a hand-built stop with one bus of each kind."""

from __future__ import annotations

import gzip
import json
from datetime import UTC, date, datetime
from typing import Any

import jsonschema
from google.transit import gtfs_realtime_pb2 as pb
from hypothesis import given
from hypothesis import strategies as st

from ghostbus_core import schemas
from ghostbus_core.departures import (
    POINTER_KEY,
    SHARDS,
    Departure,
    RouteHistory,
    StopInfo,
    Tally,
    Trip,
    departures_doc,
    expected_at,
    history_from_route_doc,
    index_docs,
    live_view,
    probability,
    read_shard,
    read_trip_updates,
    shard_key,
    shard_of,
)
from ghostbus_core.export import Cell, route_doc


def epoch(hhmm: str, day: int = 19) -> int:
    h, m = map(int, hhmm.split(":"))
    return int(datetime(2026, 11, day, h, m, tzinfo=UTC).timestamp())  # November: Dublin = UTC


NOW = epoch("08:00")
STOP = StopInfo("1358", "Merrion Square", 53.33981, -6.24891)


def trip(tid: str, start: str, route: str = "46A", rid: str = "r46a") -> Trip:
    slug = f"dublin-bus-{route.lower()}"
    return Trip(
        tid,
        rid,
        "0",
        "20261119",
        f"{start}:00",
        epoch(start),
        "Dublin Bus",
        route,
        "Dun Laoghaire",
        slug,
    )


T_TRACKED = trip("t1", "07:50")  # tracked, 2 min late at the previous stop
T_ASSIGNED = trip("t2", "08:00")  # vehicle named, not reporting
T_CANCELLED = trip("t3", "08:05")
T_SCHEDULED = trip("t4", "08:20")  # nothing live
T_NEWID = trip("t5", "08:10", route="39", rid="r39")  # live feed uses a regenerated trip id
T_PASSED = trip("t6", "07:40")  # its bus is already past this stop
T_SKIPS = trip("t7", "08:12")
DEPS = [
    Departure("1358", epoch("08:05"), 5, T_TRACKED),
    Departure("1358", epoch("08:06"), 5, T_ASSIGNED),
    Departure("1358", epoch("08:12"), 5, T_CANCELLED),
    Departure("1358", epoch("08:26"), 5, T_SCHEDULED),
    Departure("1358", epoch("08:16"), 5, T_NEWID),
    Departure("1358", epoch("07:58"), 5, T_PASSED),
    Departure("1358", epoch("08:18"), 5, T_SKIPS),
    Departure("1358", epoch("10:30"), 5, trip("t8", "10:20")),  # beyond the 60-min window
]


def entity_tu(
    tid: str,
    t: Trip,
    *,
    cancel: bool = False,
    vehicle: str | None = None,
    updates: list[tuple[int, int | None, bool]] | None = None,
) -> pb.FeedEntity:
    e = pb.FeedEntity(id=tid)
    d = e.trip_update.trip
    d.trip_id, d.route_id, d.direction_id = tid, t.route_id, int(t.direction_id)
    d.start_date, d.start_time = t.start_date, t.start_time
    d.schedule_relationship = pb.TripDescriptor.CANCELED if cancel else pb.TripDescriptor.SCHEDULED
    if vehicle:
        e.trip_update.vehicle.id = vehicle
    for seq, delay, skipped in updates or []:
        u = e.trip_update.stop_time_update.add()
        u.stop_sequence = seq
        if skipped:
            u.schedule_relationship = pb.TripUpdate.StopTimeUpdate.SKIPPED
        elif delay is not None:
            u.departure.delay = delay
    return e


def entity_vp(tid: str, t: Trip, seq: int) -> pb.FeedEntity:
    e = pb.FeedEntity(id=f"v{tid}")
    v = e.vehicle
    v.trip.trip_id, v.trip.route_id, v.trip.direction_id = tid, t.route_id, int(t.direction_id)
    v.trip.start_date, v.trip.start_time = t.start_date, t.start_time
    v.vehicle.id = f"bus-{tid}"
    v.position.latitude, v.position.longitude = 53.34, -6.25
    v.timestamp = NOW - 30
    v.current_stop_sequence = seq
    return e


def feeds(age: int = 60) -> tuple[pb.FeedMessage, pb.FeedMessage]:
    tu, vp = pb.FeedMessage(), pb.FeedMessage()
    for m in (tu, vp):
        m.header.gtfs_realtime_version = "2.0"
        m.header.timestamp = NOW - age
    tu.entity.extend(
        [
            entity_tu("t1", T_TRACKED, vehicle="bus-t1", updates=[(4, 120, False)]),
            entity_tu("t2", T_ASSIGNED, vehicle="bus-t2"),
            entity_tu("t3", T_CANCELLED, cancel=True),
            entity_tu("NEW-ID-5", T_NEWID, vehicle="bus-t5", updates=[(5, -60, False)]),
            entity_tu("t6", T_PASSED, vehicle="bus-t6"),
            entity_tu("t7", T_SKIPS, vehicle="bus-t7", updates=[(5, None, True)]),
        ]
    )
    vp.entity.extend([entity_vp("t1", T_TRACKED, 4), entity_vp("t6", T_PASSED, 7)])
    return tu, vp


def history(ghost: int = 4, phantom: int = 2, early: int = 3) -> RouteHistory:
    total = Tally(n=100, ran=85, ghost=ghost, phantom=phantom, early=early)
    return RouteHistory(total, {("weekday", 8): Tally(40, 32, 3, 2, 2)})


HISTORY = {"dublin-bus-46a": history(), "dublin-bus-39": None}


def doc(age: int = 60, **kw: int) -> dict[str, Any]:
    return departures_doc(STOP, DEPS, live_view(*feeds(age)), HISTORY, NOW, **kw)


def by_trip(d: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {r["trip_id"]: r for r in d["departures"]}


# --- the index ------------------------------------------------------------------------------------


def test_index_round_trip_is_complete_and_deterministic() -> None:
    other = StopInfo("2001", "Somewhere Else", 53.3, -6.3)
    deps = [*DEPS, Departure("2001", epoch("09:00"), 2, T_SCHEDULED)]
    docs = index_docs([STOP, other], deps, "20261119T042000Z", ["v1", "v1"])
    assert len(docs) == SHARDS + 1 and list(docs)[-1] == POINTER_KEY  # pointer written last
    info, back = read_shard(docs[shard_key("20261119T042000Z", shard_of("1358"))])
    assert info["1358"] == STOP
    assert sorted(back["1358"], key=lambda d: d.at) == sorted(DEPS, key=lambda d: d.at)
    pointer = json.loads(docs[POINTER_KEY])
    jsonschema.validate(pointer, schemas.load(schemas.STOP_INDEX_POINTER))
    assert (pointer["stops"], pointer["departures"], pointer["feed_versions"]) == (2, 9, ["v1"])
    again = index_docs([other, STOP], list(reversed(deps)), "20261119T042000Z", ["v1"])
    assert again == docs  # input order never changes a byte (gzip mtime is fixed)


def test_every_shard_is_written_even_when_empty() -> None:
    docs = index_docs([STOP], DEPS, "20261119T042000Z", [])
    empty = shard_key("20261119T042000Z", (shard_of("1358") + 1) % SHARDS)
    assert json.loads(gzip.decompress(docs[empty]))["stops"] == {}


def test_shard_is_stable() -> None:
    assert shard_of("1358") == shard_of("1358") and 0 <= shard_of("1358") < SHARDS
    assert len({shard_of(str(c)) for c in range(5000)}) == SHARDS  # spreads over all shards


# --- live matching and expected times -------------------------------------------------------------


def test_matching_falls_back_to_route_and_start_time_after_a_republish() -> None:
    rows = by_trip(doc())
    assert rows["t1"]["matched_by"] == "trip_id"
    assert rows["t5"]["matched_by"] == "route_and_start_time"  # live id NEW-ID-5, ours t5
    assert rows["t5"]["status"] == "assigned" and rows["t5"]["delay_s"] == -60


def test_delay_propagates_from_an_earlier_stop_only() -> None:
    tu, _ = feeds()
    trips = {lt.trip_id: lt for lt in read_trip_updates(tu)}
    d = DEPS[0]
    assert expected_at(d, trips["t1"]) == (d.at + 120, 120, False)  # from stop 4
    later = Departure("1358", d.at, 3, T_TRACKED)  # this stop is before the update at 4
    assert expected_at(later, trips["t1"]) == (None, None, False)
    assert expected_at(DEPS[6], trips["t7"]) == (None, None, True)


def test_statuses() -> None:
    rows = by_trip(doc())
    assert {t: r["status"] for t, r in rows.items()} == {
        "t1": "tracked",
        "t2": "assigned",
        "t3": "cancelled",
        "t4": "scheduled",
        "t5": "assigned",
        "t7": "skips_stop",
    }
    assert "t6" not in rows  # vehicle at stop 7, past our stop 5
    assert "t8" not in rows  # outside the window
    assert rows["t1"]["expected"] == "2026-11-19T08:07:00Z"
    assert [r["trip_id"] for r in doc()["departures"]][:2] == ["t2", "t1"]  # by expected time


def test_document_matches_its_schema_and_respects_the_limit() -> None:
    d = doc(limit=3)
    jsonschema.validate(d, schemas.load(schemas.DEPARTURES))
    assert len(d["departures"]) == 3 and d["live"]["status"] == "fresh"


# --- the probability ------------------------------------------------------------------------------


def test_probability_rises_as_live_evidence_rules_outcomes_out() -> None:
    rows = by_trip(doc())
    assert rows["t3"]["probability"] == 0.0 and rows["t7"]["probability"] == 0.0
    p_tracked, p_assigned = rows["t1"]["probability"], rows["t2"]["probability"]
    assert p_tracked > p_assigned > rows["t4"]["probability"] > 0
    assert rows["t5"]["probability"] is None  # route 39 has no published history
    assert rows["t5"]["basis"]["reason"].startswith("no published history")
    assert rows["t1"]["basis"] == {
        "day_type": "weekday",
        "hour": 7,
        "trips_this_hour": 0,
        "trips_on_route": 91,
    }


def test_near_departures_rule_out_early_cancellations() -> None:
    h = history(early=10)
    soon = Departure("1358", NOW + 10 * 60, 5, T_SCHEDULED)
    later = Departure("1358", NOW + 40 * 60, 5, T_SCHEDULED)
    p_soon, _ = probability(h, soon, "scheduled", NOW, live_fresh=True)
    p_later, _ = probability(h, later, "scheduled", NOW, live_fresh=True)
    p_blind, _ = probability(h, soon, "scheduled", NOW, live_fresh=False)
    assert p_soon is not None and p_later is not None
    assert p_soon > p_later == p_blind


def test_history_reads_the_published_route_file() -> None:
    day = date(2026, 11, 19)
    cells = [
        Cell(
            day,
            "Dublin Bus",
            "46A",
            8,
            n=40,
            ran=30,
            ghost=4,
            phantom=2,
            vanished=2,
            cancelled_early=2,
        )
    ]
    h = history_from_route_doc(route_doc("Dublin Bus", "46A", cells, {day: "ok"}, day))
    assert h.total == Tally(40, 30, 4, 2, 2) and h.cells[("weekday", 8)] == h.total


@given(
    st.integers(0, 500),
    st.integers(0, 500),
    st.integers(0, 500),
    st.integers(0, 500),
    st.integers(0, 500),
    st.sampled_from(["tracked", "assigned", "scheduled", "no_live_data"]),
    st.integers(-600, 7200),
    st.booleans(),
)
def test_probability_is_always_a_probability(
    ran: int,
    ghost: int,
    phantom: int,
    early: int,
    other: int,
    status: str,
    ahead: int,
    fresh: bool,
) -> None:
    n = ran + ghost + phantom + early + other
    h = RouteHistory(
        Tally(n, ran, ghost, phantom, early), {("weekday", 7): Tally(n, ran, ghost, phantom, early)}
    )
    p, basis = probability(h, Departure("1358", NOW + ahead, 5, T_TRACKED), status, NOW, fresh)
    assert p is None or 0.0 <= p <= 1.0
    assert (p is None) == ("reason" in basis)


# --- live freshness -------------------------------------------------------------------------------


def test_stale_live_data_is_labelled_and_old_live_data_is_not_used() -> None:
    assert doc(age=600)["live"]["status"] == "stale"
    assert by_trip(doc(age=600))["t3"]["status"] == "cancelled"
    old = doc(age=1200)
    assert old["live"]["status"] == "unavailable"
    assert {r["status"] for r in old["departures"]} == {"no_live_data"}
    blind = departures_doc(STOP, DEPS, None, HISTORY, NOW)
    assert blind["live"] == {
        "status": "unavailable",
        "trip_updates_at": None,
        "vehicles_at": None,
        "age_s": None,
    }
