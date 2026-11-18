"""Archive and NTA client, with no network and no Docker."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from ghostbus_core.feeds import Feed
from ghostbus_core.pacing import FetchStatus
from ghostbus_ingest.archive import (
    LocalArchive,
    S3Archive,
    archive_from_env,
    archive_from_uri,
    compress,
    decompress,
)
from ghostbus_ingest.nta_client import NtaClient

# --- archive ---------------------------------------------------------------------------------


def test_compress_round_trip() -> None:
    data = b"gtfs" * 10_000
    packed = compress(data)
    assert len(packed) < len(data) // 10
    assert decompress(packed) == data


def test_local_archive(tmp_path: Path) -> None:
    a = LocalArchive(tmp_path)
    a.put_bytes("raw/feed=x/dt=2026-11-15/hh=19/b.bin", b"2", "application/octet-stream")
    a.put_bytes("raw/feed=x/dt=2026-11-15/hh=19/a.bin", b"1", "application/octet-stream")
    a.put_bytes("raw/feed=y/z.bin", b"3", "application/octet-stream")
    assert a.get_bytes("raw/feed=x/dt=2026-11-15/hh=19/a.bin") == b"1"
    assert a.get_bytes("missing") is None
    assert a.exists("raw/feed=y/z.bin") and not a.exists("raw/feed=y")
    assert list(a.list_keys("raw/feed=x/")) == [
        "raw/feed=x/dt=2026-11-15/hh=19/a.bin",
        "raw/feed=x/dt=2026-11-15/hh=19/b.bin",
    ]
    assert list(a.list_keys("raw/feed=nope/")) == []
    assert not list(tmp_path.rglob(".tmp-*"))


def test_local_archive_overwrite_and_put_file(tmp_path: Path) -> None:
    a = LocalArchive(tmp_path / "archive")
    src = tmp_path / "src.zip"
    src.write_bytes(b"zip")
    a.put_file("raw/feed=static/x.zip", src, "application/zip")
    a.put_bytes("raw/feed=static/x.zip", b"zip2", "application/zip")
    assert a.get_bytes("raw/feed=static/x.zip") == b"zip2"


@pytest.mark.parametrize("key", ["../escape", "raw/../../escape", "/abs"])
def test_local_archive_rejects_unsafe_keys(tmp_path: Path, key: str) -> None:
    with pytest.raises(ValueError):
        LocalArchive(tmp_path).put_bytes(key, b"", "x")


def test_archive_from_uri(tmp_path: Path) -> None:
    local = archive_from_uri(tmp_path.as_uri())
    assert isinstance(local, LocalArchive) and local.root == tmp_path
    s3 = archive_from_uri("s3://bucket/some/prefix", s3_endpoint="http://localhost:8333")
    assert isinstance(s3, S3Archive)
    assert s3.uri == "s3://bucket/some/prefix/"
    with pytest.raises(ValueError):
        archive_from_uri("ftp://nope")
    with pytest.raises(ValueError):
        archive_from_env({})


# --- NTA client --------------------------------------------------------------------------------


def _client(handler: httpx.MockTransport) -> NtaClient:
    return NtaClient("test-key", transport=handler)


def test_client_sends_key_and_returns_body() -> None:
    seen: list[httpx.Request] = []

    def handle(req: httpx.Request) -> httpx.Response:
        seen.append(req)
        return httpx.Response(200, content=b"\x0a\x00")

    res = _client(httpx.MockTransport(handle)).fetch(Feed.VEHICLES)
    assert res.status is FetchStatus.OK and res.body == b"\x0a\x00" and res.http_status == 200
    assert seen[0].headers["x-api-key"] == "test-key"
    assert str(seen[0].url) == Feed.VEHICLES.url
    assert len(seen) == 1  # no hidden retries


def test_client_429_reads_retry_after() -> None:
    def handle(req: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"Retry-After": "120"}, text="Rate limit is exceeded")

    res = _client(httpx.MockTransport(handle)).fetch(Feed.TRIP_UPDATES)
    assert res.status is FetchStatus.THROTTLED
    assert res.retry_after_s == 120
    assert res.body == b"" and res.error == "Rate limit is exceeded"


def test_client_server_error_is_not_retried() -> None:
    calls = 0

    def handle(req: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503)

    res = _client(httpx.MockTransport(handle)).fetch(Feed.ALERTS)
    assert res.status is FetchStatus.SERVER_ERROR and calls == 1


def test_client_network_error() -> None:
    def handle(req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("timed out", request=req)

    res = _client(httpx.MockTransport(handle)).fetch(Feed.VEHICLES)
    assert res.status is FetchStatus.NETWORK_ERROR and res.http_status is None
    assert res.error is not None and "ConnectTimeout" in res.error


def test_client_rejects_empty_key() -> None:
    with pytest.raises(ValueError):
        NtaClient("")


def test_local_create_if_absent(tmp_path: Path) -> None:
    from ghostbus_ingest.archive import LocalArchive

    a = LocalArchive(tmp_path)
    assert a.create_if_absent("claims/minute=7", b"first")
    assert not a.create_if_absent("claims/minute=7", b"second")
    assert a.get_bytes("claims/minute=7") == b"first"


def test_the_api_key_never_reaches_a_stored_error() -> None:
    """Heartbeats store error text in S3, Kafka and logs. Even when an error message or an NTA
    error page contains the key, the stored error must not."""
    key = "fake-key-for-the-redaction-test"

    def echoing_page(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, text=f"invalid key {request.headers['x-api-key']}")

    def echoing_exception(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError(f"proxy refused request with x-api-key={key}")

    for handler in (echoing_page, echoing_exception):
        res = NtaClient(key, transport=httpx.MockTransport(handler)).fetch(Feed.VEHICLES)
        assert res.error is not None and key not in res.error and "[REDACTED]" in res.error
