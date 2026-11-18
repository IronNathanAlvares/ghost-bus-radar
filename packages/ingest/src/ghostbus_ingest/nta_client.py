"""HTTP client for the NTA GTFS-R v2 API. One method, one call, no hidden retries.

Retries are deliberately absent: under the usage policy a retry is another call, and calls are
budgeted one per minute (``ghostbus_core.feeds``). A failed call is reported, not repeated.
The API key is sent in the ``x-api-key`` header and never logged.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Protocol

import httpx

from ghostbus_core.feeds import Feed
from ghostbus_core.pacing import FetchStatus

USER_AGENT = "ghost-bus-radar/0.1 (open-source reliability study)"


@dataclass(frozen=True)
class FetchResult:
    status: FetchStatus
    http_status: int | None
    body: bytes
    elapsed_ms: int
    retry_after_s: float | None = None
    error: str | None = None


class FeedFetcher(Protocol):
    def fetch(self, feed: Feed) -> FetchResult: ...


def _retry_after(headers: httpx.Headers) -> float | None:
    value = headers.get("retry-after")
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None  # HTTP-date form; rare, and the backoff covers it anyway


class NtaClient:
    def __init__(
        self,
        api_key: str,
        *,
        timeout_s: float = 20.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        if not api_key:
            raise ValueError("NTA API key is empty")
        self._key = api_key
        self._http = httpx.Client(
            headers={
                "x-api-key": api_key,
                "Accept": "application/x-protobuf",
                "User-Agent": USER_AGENT,
            },
            timeout=httpx.Timeout(timeout_s, connect=10.0),
            transport=transport,
        )

    def fetch(self, feed: Feed) -> FetchResult:
        t0 = time.monotonic()
        try:
            resp = self._http.get(feed.url)
        except httpx.HTTPError as exc:
            return FetchResult(
                status=FetchStatus.NETWORK_ERROR,
                http_status=None,
                body=b"",
                elapsed_ms=round((time.monotonic() - t0) * 1000),
                error=self._redact(f"{type(exc).__name__}: {exc}"),
            )
        elapsed_ms = round((time.monotonic() - t0) * 1000)
        status = FetchStatus.from_http(resp.status_code)
        if status is FetchStatus.OK:
            return FetchResult(status, resp.status_code, resp.content, elapsed_ms)
        return FetchResult(
            status=status,
            http_status=resp.status_code,
            body=b"",
            elapsed_ms=elapsed_ms,
            retry_after_s=_retry_after(resp.headers),
            error=self._redact(resp.text[:300]),
        )

    def _redact(self, text: str) -> str:
        """Error text is stored in heartbeats (S3, Kafka, logs). Nothing should ever echo the key,
        but nothing guarantees it either: a proxy, an exception message or an NTA error page might.
        So every stored error has the key removed (docs/07-security.md)."""
        return text.replace(self._key, "[REDACTED]") if self._key else text

    def close(self) -> None:
        self._http.close()
