"""API Gateway HTTP API (payload format 2.0) entry point.

Sprint 0 only serves ``GET /v1/health`` so the deploy path can be proven before any data exists.
"""

from __future__ import annotations

import json
from typing import Any

import ghostbus_core

JSON_HEADERS = {"content-type": "application/json", "cache-control": "no-store"}


def _response(status: int, body: dict[str, Any]) -> dict[str, Any]:
    return {"statusCode": status, "headers": JSON_HEADERS, "body": json.dumps(body)}


def handler(event: dict[str, Any], context: object) -> dict[str, Any]:
    route = event.get("routeKey", "")
    if route == "GET /v1/health":
        return _response(200, {"status": "ok", "version": ghostbus_core.__version__})
    return _response(404, {"error": "not_found", "route": route})
