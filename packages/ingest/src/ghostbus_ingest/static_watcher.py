"""Static GTFS watcher: archive every version of ``GTFS_Realtime.zip`` the NTA publishes.

Realtime trip and route ids only match the static file of the same version, and NTA may publish a
new one any day, so every version must be kept (SCD type 2 in Sprint 2 is built from these).

Each check is one HEAD request (not the GTFS-R API, so outside its one-call-per-minute budget).
Only when ``ETag``, ``Last-Modified`` or ``Content-Length`` changes is the ~100 MB file downloaded,
hashed, and stored under its SHA-256 with a manifest. Identical content re-published with new
headers is recognised by hash and not stored twice.

Usage::

    python -m ghostbus_ingest.static_watcher            # check hourly, forever
    python -m ghostbus_ingest.static_watcher --once
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import random
import signal
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from types import FrameType

import httpx

from ghostbus_core.archive_layout import STATIC_FILENAME, STATIC_STATE_KEY, static_prefix
from ghostbus_core.feeds import STATIC_GTFS_URL
from ghostbus_core.static_feed import read_feed_info
from ghostbus_ingest.archive import Archive, archive_from_env
from ghostbus_ingest.nta_client import USER_AGENT

log = logging.getLogger("ghostbus.static_watcher")

CHECK_INTERVAL_S = 3600


@dataclass(frozen=True)
class StaticCheck:
    changed_headers: bool
    sha256: str | None
    new_version: bool
    feed_version: str | None


def _validators(headers: httpx.Headers) -> dict[str, str | None]:
    return {
        "etag": headers.get("etag"),
        "last_modified": headers.get("last-modified"),
        "content_length": headers.get("content-length"),
    }


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def check_once(
    http: httpx.Client, archive: Archive, url: str = STATIC_GTFS_URL, workdir: Path | None = None
) -> StaticCheck:
    raw_state = archive.get_bytes(STATIC_STATE_KEY)
    state: dict[str, object] = json.loads(raw_state) if raw_state else {}

    head = http.head(url)
    head.raise_for_status()
    validators = _validators(head.headers)
    checked_at = _now_ms()
    if state.get("validators") == validators and state.get("sha256"):
        state["checked_at_ms"] = checked_at
        archive.put_bytes(STATIC_STATE_KEY, json.dumps(state).encode(), "application/json")
        return StaticCheck(False, str(state["sha256"]), False, _str(state.get("feed_version")))

    with tempfile.TemporaryDirectory(dir=workdir) as tmp:
        path = Path(tmp) / STATIC_FILENAME
        digest = hashlib.sha256()
        size = 0
        with http.stream("GET", url) as resp, path.open("wb") as f:
            resp.raise_for_status()
            validators = _validators(resp.headers)  # the headers of what we actually got
            for chunk in resp.iter_bytes(1 << 20):
                f.write(chunk)
                digest.update(chunk)
                size += len(chunk)
        sha = digest.hexdigest()
        prefix = static_prefix(sha)
        feed_info = read_feed_info(path)
        new_version = not archive.exists(prefix + "manifest.json")
        if new_version:
            archive.put_file(prefix + STATIC_FILENAME, path, "application/zip")
            manifest = {
                "sha256": sha,
                "bytes": size,
                "url": url,
                "fetched_at_ms": checked_at,
                **validators,
                "feed_info": feed_info,
            }
            # The manifest goes last: its presence means the zip is complete.
            archive.put_bytes(
                prefix + "manifest.json",
                json.dumps(manifest, indent=2).encode(),
                "application/json",
            )

    feed_version = feed_info.get("feed_version")
    state = {
        "validators": validators,
        "sha256": sha,
        "feed_version": feed_version,
        "checked_at_ms": checked_at,
        "changed_at_ms": checked_at if new_version else state.get("changed_at_ms"),
    }
    archive.put_bytes(STATIC_STATE_KEY, json.dumps(state).encode(), "application/json")
    return StaticCheck(True, sha, new_version, feed_version)


def _str(v: object) -> str | None:
    return None if v is None else str(v)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--interval-s", type=float, default=CHECK_INTERVAL_S)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)

    archive = archive_from_env(os.environ)
    stop = threading.Event()

    def _stop(signum: int, _frame: FrameType | None) -> None:
        stop.set()

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    timeout = httpx.Timeout(60.0, connect=15.0)
    with httpx.Client(
        headers={"User-Agent": USER_AGENT}, timeout=timeout, follow_redirects=True
    ) as http:
        while True:
            try:
                r = check_once(http, archive)
                log.info(
                    "static: headers_changed=%s new_version=%s sha256=%s feed_version=%s",
                    r.changed_headers,
                    r.new_version,
                    (r.sha256 or "")[:12],
                    r.feed_version,
                )
            except Exception:
                log.exception("static check failed; will retry next interval")
                if args.once:
                    return 1
            if args.once:
                return 0
            # Jitter so checks do not land on the hour with everyone else's cron jobs.
            if stop.wait(args.interval_s + random.uniform(-60, 60)):
                return 0


if __name__ == "__main__":
    sys.exit(main())
