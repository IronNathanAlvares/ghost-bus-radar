from __future__ import annotations

import hashlib
import io
import json
import zipfile
from pathlib import Path

import httpx

from ghostbus_core.archive_layout import STATIC_FILENAME, STATIC_STATE_KEY, static_prefix
from ghostbus_core.static_feed import read_feed_info
from ghostbus_ingest.archive import LocalArchive
from ghostbus_ingest.static_watcher import check_once

URL = "https://example.test/GTFS_Realtime.zip"


def make_zip(feed_version: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(
            "feed_info.txt",
            "﻿feed_publisher_name,feed_version,feed_start_date,feed_end_date\n"
            f"National Transport Authority,{feed_version},20261114,20271114\n",
        )
        zf.writestr("routes.txt", "route_id,agency_id\n1,7778019\n")
    return buf.getvalue()


class FakeSite:
    def __init__(self) -> None:
        self.body = make_zip("V1")
        self.etag = '"1"'
        self.gets = 0

    def handle(self, req: httpx.Request) -> httpx.Response:
        headers = {"ETag": self.etag, "Last-Modified": "Sat, 14 Nov 2026 23:09:02 GMT"}
        if req.method == "HEAD":
            return httpx.Response(200, headers=headers | {"Content-Length": str(len(self.body))})
        self.gets += 1
        return httpx.Response(200, headers=headers, content=self.body)


def test_watcher_stores_each_version_once(tmp_path: Path) -> None:
    site = FakeSite()
    archive = LocalArchive(tmp_path / "archive")
    http = httpx.Client(transport=httpx.MockTransport(site.handle))

    first = check_once(http, archive, URL, workdir=tmp_path)
    sha1 = hashlib.sha256(site.body).hexdigest()
    assert first.new_version and first.sha256 == sha1 and first.feed_version == "V1"
    stored = archive.get_bytes(static_prefix(sha1) + STATIC_FILENAME)
    assert stored == site.body
    manifest = json.loads(archive.get_bytes(static_prefix(sha1) + "manifest.json") or b"")
    assert manifest["feed_info"]["feed_version"] == "V1" and manifest["etag"] == '"1"'

    # Unchanged headers: a HEAD only, no download.
    again = check_once(http, archive, URL, workdir=tmp_path)
    assert not again.changed_headers and not again.new_version and site.gets == 1

    # New headers, same bytes: downloaded, recognised by hash, not stored twice.
    site.etag = '"2"'
    same = check_once(http, archive, URL, workdir=tmp_path)
    assert same.changed_headers and not same.new_version and site.gets == 2

    # A real new version.
    site.body, site.etag = make_zip("V2"), '"3"'
    new = check_once(http, archive, URL, workdir=tmp_path)
    assert new.new_version and new.feed_version == "V2"
    manifests = [k for k in archive.list_keys("raw/feed=static/") if k.endswith("manifest.json")]
    assert len(manifests) == 2
    state = json.loads(archive.get_bytes(STATIC_STATE_KEY) or b"")
    assert state["feed_version"] == "V2"


def test_read_feed_info_strips_bom(tmp_path: Path) -> None:
    path = tmp_path / "g.zip"
    path.write_bytes(make_zip("ABC"))
    assert read_feed_info(path)["feed_publisher_name"] == "National Transport Authority"
