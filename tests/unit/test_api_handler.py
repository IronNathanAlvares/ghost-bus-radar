import json

from ghostbus_api.handler import handler


def test_health() -> None:
    response = handler({"routeKey": "GET /v1/health"}, None)
    assert response["statusCode"] == 200
    assert json.loads(response["body"])["status"] == "ok"


def test_unknown_route_is_404() -> None:
    response = handler({"routeKey": "GET /nope"}, None)
    assert response["statusCode"] == 404
