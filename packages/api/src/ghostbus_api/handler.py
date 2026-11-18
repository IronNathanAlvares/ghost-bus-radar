"""API Gateway HTTP API (payload format 2.0) entry point: "will my bus come?" (ADR-0012).

    GET /v1/health
    GET /v1/stops/{code}/departures?minutes=60&limit=10

Reads only S3, never the NTA (one poller per API key): the stop index (``INDEX_URI``), the
newest archived live snapshots (``ARCHIVE_URI``) and the routes' published history
(``SITE_DATA_URI``). Everything read is cached in the container for as long as it can be valid,
so a warm request usually costs one S3 list and one small read.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from collections import OrderedDict
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from typing import Any

from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2 as pb

import ghostbus_core
from ghostbus_core.departures import (
    POINTER_KEY,
    Departure,
    Live,
    RouteHistory,
    StopInfo,
    departures_doc,
    history_from_route_doc,
    live_view,
    read_shard,
    shard_key,
    shard_of,
)
from ghostbus_core.model import Model
from ghostbus_ingest.archive import Archive, archive_from_uri, decompress

log = logging.getLogger("ghostbus.api")
log.setLevel(logging.INFO)

ROUTES = ("GET /v1/health", "GET /v1/stops/{code}/departures")
CODE_RE = re.compile(r"^[0-9A-Za-z]{1,12}$")
MINUTES = (1, 180, 60)  # min, max, default
LIMIT = (1, 30, 10)
POINTER_TTL_S = 300
NEWEST_TTL_S = 20  # how long "the newest snapshot is X" is trusted before listing again
HISTORY_TTL_S = 3600
MAX_SHARDS = 64
MAX_ROUTES = 512

JSON_HEADERS = {"content-type": "application/json"}


def _response(status: int, body: Mapping[str, Any], max_age: int = 0) -> dict[str, Any]:
    cache = f"public, max-age={max_age}" if max_age else "no-store"
    return {
        "statusCode": status,
        "headers": JSON_HEADERS | {"cache-control": cache},
        "body": json.dumps(body, separators=(",", ":"), ensure_ascii=False),
    }


def _error(status: int, error: str, detail: str) -> dict[str, Any]:
    return _response(status, {"error": error, "detail": detail})


class ApiError(Exception):
    def __init__(self, status: int, error: str, detail: str) -> None:
        super().__init__(detail)
        self.status, self.error, self.detail = status, error, detail


class Sources:
    """Cached reads from S3. One per Lambda container; the clock is injectable for tests."""

    def __init__(
        self,
        index: Archive,
        archive: Archive,
        site: Archive,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.index, self.archive, self.site, self.clock = index, archive, site, clock
        self._pointer: tuple[float, dict[str, Any]] | None = None
        self._shards: OrderedDict[str, tuple[dict[str, StopInfo], dict[str, list[Departure]]]] = (
            OrderedDict()
        )
        self._newest: dict[str, tuple[float, str | None]] = {}
        self._decoded: dict[str, tuple[str, pb.FeedMessage | None]] = {}
        self._history: OrderedDict[str, tuple[float, RouteHistory | None]] = OrderedDict()
        self._live: tuple[tuple[str | None, str | None], Live] | None = None
        self._model: tuple[float, Model | None] | None = None
        #: Per-stage timings of the last request, for the request log line.
        self.stages: dict[str, float] = {}
        self.s3_reads = 0

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Sources:
        endpoint = env.get("ARCHIVE_S3_ENDPOINT")
        return cls(
            archive_from_uri(env["INDEX_URI"], endpoint),
            archive_from_uri(env["ARCHIVE_URI"], endpoint),
            archive_from_uri(env["SITE_DATA_URI"], endpoint),
        )

    def _get(self, store: Archive, key: str) -> bytes | None:
        self.s3_reads += 1
        return store.get_bytes(key)

    def pointer(self) -> dict[str, Any]:
        now = self.clock()
        if self._pointer is None or now - self._pointer[0] > POINTER_TTL_S:
            raw = self._get(self.index, POINTER_KEY)
            if raw is None:
                raise ApiError(503, "index_unavailable", "the stop index has not been built yet")
            self._pointer = (now, json.loads(raw))
        return self._pointer[1]

    def stop(self, code: str) -> tuple[StopInfo, list[Departure]]:
        pointer = self.pointer()
        key = shard_key(pointer["build"], shard_of(code))
        shard = self._shards.get(key)
        if shard is None:
            raw = self._get(self.index, key)
            if raw is None:
                raise ApiError(503, "index_unavailable", "a stop-index shard is missing")
            shard = read_shard(raw)
            self._shards[key] = shard
            while len(self._shards) > MAX_SHARDS:
                self._shards.popitem(last=False)
        else:
            self._shards.move_to_end(key)
        info, deps = shard
        if code not in info:
            raise ApiError(404, "unknown_stop", f"no bus stop with code {code}")
        return info[code], deps[code]

    def _newest_key(self, feed: str) -> str | None:
        now = self.clock()
        hit = self._newest.get(feed)
        if hit is not None and now - hit[0] <= NEWEST_TTL_S:
            return hit[1]
        t = datetime.fromtimestamp(now, UTC)
        newest = None
        for hour in (t, t - timedelta(hours=1)):  # just after the hour, the newest is in the last
            prefix = f"raw/feed={feed}/dt={hour:%Y-%m-%d}/hh={hour:%H}/"
            keys = [k for k in self.archive.list_keys(prefix) if k.endswith(".pb.zst")]
            if keys:
                newest = max(keys)
                break
        self._newest[feed] = (now, newest)
        return newest

    def snapshot(self, feed: str) -> pb.FeedMessage | None:
        key = self._newest_key(feed)
        if key is None:
            return None
        cached = self._decoded.get(feed)
        if cached is not None and cached[0] == key:
            return cached[1]
        raw = self._get(self.archive, key)
        msg: pb.FeedMessage | None = None
        if raw is not None:
            try:
                msg = pb.FeedMessage()
                msg.ParseFromString(decompress(raw))
            except (DecodeError, ValueError):
                log.warning("undecodable snapshot %s", key)
                msg = None
        self._decoded[feed] = (key, msg)
        return msg

    def live(self) -> Live:
        """The live view, rebuilt only when a newer snapshot arrives (about once a minute)."""
        tu, vp = self.snapshot("trip_updates"), self.snapshot("vehicles")
        key = (
            self._decoded.get("trip_updates", ("", None))[0] if tu else None,
            self._decoded.get("vehicles", ("", None))[0] if vp else None,
        )
        if self._live is None or self._live[0] != key:
            self._live = (key, live_view(tu, vp))
        return self._live[1]

    def histories(self, slugs: set[str]) -> dict[str, RouteHistory | None]:
        """Route files for every route at a stop, the missing ones fetched in parallel (a busy
        stop has ~20 routes; one after another that is ~1 s of S3 round trips)."""
        now = self.clock()
        out: dict[str, RouteHistory | None] = {}
        todo = []
        for s in slugs:
            hit = self._history.get(s)
            if hit is not None and now - hit[0] <= HISTORY_TTL_S:
                out[s] = hit[1]
            else:
                todo.append(s)
        if todo:
            with ThreadPoolExecutor(max_workers=min(8, len(todo))) as pool:
                for s, value in zip(todo, pool.map(self._fetch_history, todo), strict=True):
                    out[s] = value
                    self._history[s] = (now, value)
            while len(self._history) > MAX_ROUTES:
                self._history.popitem(last=False)
        return out

    def model(self) -> Model | None:
        """The fitted model (``data/v1/model.json``, heuristic-v2), refreshed hourly. Without it
        (before the first export) the API answers with heuristic-v1."""
        now = self.clock()
        if self._model is None or now - self._model[0] > HISTORY_TTL_S:
            raw = self._get(self.site, "model.json")
            self._model = (now, Model.from_doc(json.loads(raw)) if raw else None)
        return self._model[1]

    def _fetch_history(self, slug: str) -> RouteHistory | None:
        raw = self._get(self.site, f"routes/{slug}.json")
        return history_from_route_doc(json.loads(raw)) if raw else None


SOURCES: Sources | None = None


def _sources() -> Sources:
    global SOURCES
    if SOURCES is None:
        SOURCES = Sources.from_env(os.environ)
    return SOURCES


def _int_param(params: Mapping[str, str], name: str, bounds: tuple[int, int, int]) -> int:
    lo, hi, default = bounds
    raw = params.get(name)
    if raw is None or raw == "":
        return default
    if not raw.isdigit() or not lo <= int(raw) <= hi:
        raise ApiError(400, "bad_parameter", f"{name} must be a whole number from {lo} to {hi}")
    return int(raw)


def departures(event: Mapping[str, Any], src: Sources) -> dict[str, Any]:
    code = (event.get("pathParameters") or {}).get("code", "")
    if not CODE_RE.fullmatch(code):
        raise ApiError(400, "bad_stop_code", "a stop code is 1 to 12 letters or digits")
    params = event.get("queryStringParameters") or {}
    unknown = set(params) - {"minutes", "limit"}
    if unknown:
        raise ApiError(400, "bad_parameter", f"unknown parameter: {sorted(unknown)[0]}")
    minutes = _int_param(params, "minutes", MINUTES)
    limit = _int_param(params, "limit", LIMIT)
    t = time.perf_counter()
    stages = src.stages = {}
    stop, deps = src.stop(code)
    stages["stop"] = _ms(t)
    now = int(src.clock())
    pointer = src.pointer()
    until = pointer.get("valid_until") or pointer.get("last_departure")
    if until and now >= datetime.fromisoformat(until.replace("Z", "+00:00")).timestamp():
        raise ApiError(503, "index_expired", "the stop index is out of date")
    slugs = {d.trip.slug for d in deps if now - 1800 <= d.at <= now + minutes * 60}
    t = time.perf_counter()
    history = src.histories(slugs)
    stages["history"] = _ms(t)
    t = time.perf_counter()
    live = src.live()
    stages["live"] = _ms(t)
    t = time.perf_counter()
    body = departures_doc(stop, deps, live, history, now, minutes, limit, src.model())
    stages["answer"] = _ms(t)
    return body


def _ms(since: float) -> float:
    return round((time.perf_counter() - since) * 1000, 1)


def handler(event: dict[str, Any], context: object) -> dict[str, Any]:
    started = time.perf_counter()
    if SOURCES is not None:
        SOURCES.stages = {}
    route = event.get("routeKey", "")
    status = 500
    try:
        if route == "GET /v1/health":
            status = 200
            return _response(200, {"status": "ok", "version": ghostbus_core.__version__})
        if route == "GET /v1/stops/{code}/departures":
            src = _sources()
            body = departures(event, src)
            status = 200
            return _response(200, body, max_age=30)
        status = 404
        return _error(404, "not_found", f"no route {route}")
    except ApiError as e:
        status = e.status
        return _error(e.status, e.error, e.detail)
    except Exception:
        log.exception("unhandled error")
        status = 500
        return _error(500, "internal", "something went wrong; it has been logged")
    finally:
        # One line per request. No caller IP or headers: not needed, and personal data. The stop
        # code only when it is valid, so nothing a caller types reaches the logs unchecked.
        code = (event.get("pathParameters") or {}).get("code")
        log.info(
            json.dumps(
                {
                    "route": route if route in ROUTES else "other",
                    "code": code if isinstance(code, str) and CODE_RE.fullmatch(code) else None,
                    "status": status,
                    "ms": round((time.perf_counter() - started) * 1000, 1),
                    "s3_reads": SOURCES.s3_reads if SOURCES else 0,
                    "stages_ms": SOURCES.stages if SOURCES else {},
                }
            )
        )
