"""The API Lambda (ADR-0012) against local stores laid out exactly like S3: the stop index, the raw
archive with a real zstd snapshot, and a published route file."""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import date
from pathlib import Path
from typing import Any

import jsonschema
import pytest
from google.transit import gtfs_realtime_pb2 as pb

from ghostbus_api import handler as api
from ghostbus_api.handler import Sources, handler
from ghostbus_core import schemas
from ghostbus_core.archive_layout import snapshot_key
from ghostbus_core.departures import index_docs
from ghostbus_core.export import Cell, dumps, route_doc
from ghostbus_core.feeds import Feed
from ghostbus_ingest.archive import LocalArchive, compress
from tests.unit.test_departures import DEPS, NOW, STOP, feeds

ROUTE = "GET /v1/stops/{code}/departures"
UNTIL = NOW + 2 * 86400 - 4 * 3600  # 21 Nov 04:00: the start of the first day not covered


class Clock:
    def __init__(self, t: float) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t


@pytest.fixture
def src(tmp_path: Path) -> Iterator[Sources]:
    index, archive, site = (LocalArchive(tmp_path / n) for n in ("index", "archive", "site"))
    for key, body in index_docs([STOP], DEPS, "20261119T042000Z", ["v1"], UNTIL).items():
        index.put_bytes(key, body, "application/octet-stream")
    tu, vp = feeds()
    archive.put_bytes(
        snapshot_key(Feed.TRIP_UPDATES, (NOW - 60) * 1000),
        compress(tu.SerializeToString()),
        "application/zstd",
    )
    archive.put_bytes(
        snapshot_key(Feed.VEHICLES, (NOW - 30) * 1000),
        compress(vp.SerializeToString()),
        "application/zstd",
    )
    day = date(2026, 11, 19)
    cells = [Cell(day, "Dublin Bus", "46A", 8, n=40, ran=34, ghost=3, phantom=1, cancelled_early=2)]
    site.put_bytes(
        "routes/dublin-bus-46a.json",
        dumps(route_doc("Dublin Bus", "46A", cells, {day: "ok"}, day)),
        "application/json",
    )
    s = Sources(index, archive, site, clock=Clock(NOW))
    api.SOURCES = s
    yield s
    api.SOURCES = None


def call(code: str, **params: str) -> tuple[int, dict[str, Any]]:
    r = handler(
        {
            "routeKey": ROUTE,
            "pathParameters": {"code": code},
            "queryStringParameters": params or None,
        },
        None,
    )
    return r["statusCode"], json.loads(r["body"])


def test_health() -> None:
    response = handler({"routeKey": "GET /v1/health"}, None)
    assert response["statusCode"] == 200
    assert json.loads(response["body"])["status"] == "ok"


def test_unknown_route_is_404() -> None:
    response = handler({"routeKey": "GET /nope"}, None)
    assert response["statusCode"] == 404


def test_departures_end_to_end(src: Sources) -> None:
    status, body = call("1358")
    assert status == 200
    jsonschema.validate(body, schemas.load(schemas.DEPARTURES))
    rows = {r["trip_id"]: r for r in body["departures"]}
    assert rows["t1"]["status"] == "tracked" and rows["t3"]["probability"] == 0.0
    assert rows["t4"]["probability"] is not None  # from the published route file
    assert body["live"]["status"] == "fresh" and body["live"]["age_s"] == 60


def test_a_warm_container_reads_almost_nothing(src: Sources) -> None:
    call("1358")
    first = src.s3_reads
    call("1358")
    assert first == 6  # pointer, shard, two snapshots, two route files (39's is missing: cached)
    assert src.s3_reads == first  # all from memory within the TTLs


def test_a_new_snapshot_is_picked_up(src: Sources) -> None:
    call("1358")
    tu, _ = feeds(age=0)
    tu.entity[2].trip_update.trip.schedule_relationship = pb.TripDescriptor.SCHEDULED  # t3
    src.archive.put_bytes(
        snapshot_key(Feed.TRIP_UPDATES, NOW * 1000 + 5),
        compress(tu.SerializeToString()),
        "application/zstd",
    )
    src.clock.t += 25  # type: ignore[attr-defined]  # past the newest-key TTL
    _, body = call("1358")
    assert {r["trip_id"]: r["status"] for r in body["departures"]}["t3"] != "cancelled"


@pytest.mark.parametrize(
    ("code", "params", "status", "error"),
    [
        ("../etc", {}, 400, "bad_stop_code"),
        ("1" * 13, {}, 400, "bad_stop_code"),
        ("1358", {"minutes": "0"}, 400, "bad_parameter"),
        ("1358", {"minutes": "9999"}, 400, "bad_parameter"),
        ("1358", {"limit": "-1"}, 400, "bad_parameter"),
        ("1358", {"debug": "1"}, 400, "bad_parameter"),
        ("9999", {}, 404, "unknown_stop"),
    ],
)
def test_bad_requests_are_refused_clearly(
    src: Sources, code: str, params: dict[str, str], status: int, error: str
) -> None:
    got, body = call(code, **params)
    assert (got, body["error"]) == (status, error)


def test_no_index_yet_is_503(tmp_path: Path) -> None:
    empty = LocalArchive(tmp_path / "empty")
    api.SOURCES = Sources(empty, empty, empty, clock=Clock(NOW))
    try:
        assert call("1358") == (
            503,
            {"error": "index_unavailable", "detail": "the stop index has not been built yet"},
        )
    finally:
        api.SOURCES = None


def test_an_expired_index_is_503_not_silently_wrong(src: Sources) -> None:
    src.clock.t = UNTIL - 1  # type: ignore[attr-defined]
    assert call("1358")[0] == 200  # long after the last indexed departure: still valid
    src.clock.t = UNTIL  # type: ignore[attr-defined]
    status, body = call("1358")
    assert (status, body["error"]) == (503, "index_expired")


def test_no_live_data_still_answers_from_the_timetable(src: Sources, tmp_path: Path) -> None:
    src.archive = LocalArchive(tmp_path / "nothing")
    status, body = call("1358")
    assert status == 200 and body["live"]["status"] == "unavailable"
    assert {r["status"] for r in body["departures"]} == {"no_live_data"}
