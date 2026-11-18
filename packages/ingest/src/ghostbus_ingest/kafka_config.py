"""librdkafka configuration shared by every producer and admin client."""

from __future__ import annotations

from collections.abc import Mapping


def producer_config(bootstrap: str, client_id: str) -> dict[str, str | int | float | bool]:
    """An idempotent producer: retries can never write a message twice or out of order.

    ``enable.idempotence`` implies ``acks=all`` and bounded in-flight requests; they are set
    explicitly so the guarantee is visible in code review, not hidden in a library default.
    """
    return {
        "bootstrap.servers": bootstrap,
        "client.id": client_id,
        "enable.idempotence": True,
        "acks": "all",
        "max.in.flight.requests.per.connection": 5,
        "compression.type": "zstd",
        "linger.ms": 50,
        "delivery.timeout.ms": 120_000,
    }


def admin_config(bootstrap: str) -> dict[str, str | int | float | bool]:
    return {"bootstrap.servers": bootstrap}


def bootstrap_from_env(env: Mapping[str, str]) -> str:
    return env.get("KAFKA_BOOTSTRAP", "localhost:9092")
