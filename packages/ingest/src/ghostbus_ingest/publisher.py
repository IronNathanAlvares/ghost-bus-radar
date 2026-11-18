"""Kafka publishing for the poller, with delivery accounting for the heartbeat.

Kafka is best-effort transport (ADR-0001): the snapshot is already in the archive. If Kafka is
down the poller keeps going, the heartbeat reports unconfirmed messages, and backfill closes the
gap. ``NullPublisher`` is the collect-only mode, when ``KAFKA_BOOTSTRAP`` is empty.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Protocol

from ghostbus_ingest.kafka_config import producer_config

log = logging.getLogger(__name__)


def encode(value: dict[str, object]) -> bytes:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


class Publisher(Protocol):
    def publish(self, topic: str, key: str, value: bytes) -> None: ...

    def flush(self, timeout_s: float) -> tuple[int, int]:
        """Wait for deliveries. Returns (delivered, unconfirmed) since the previous flush."""
        ...

    def close(self) -> None: ...


class NullPublisher:
    def publish(self, topic: str, key: str, value: bytes) -> None:
        pass

    def flush(self, timeout_s: float) -> tuple[int, int]:
        return 0, 0

    def close(self) -> None:
        pass


class KafkaPublisher:
    def __init__(
        self, bootstrap: str, client_id: str = "ghostbus-poller", topic_prefix: str = ""
    ) -> None:
        """``topic_prefix`` sends everything to prefixed copies of the topics (tests, isolated
        environments on a shared cluster), never to the live ones."""
        # Imported here: the collect-mode Lambda ships without librdkafka.
        from confluent_kafka import Producer

        self._producer = Producer(producer_config(bootstrap, client_id))
        self._prefix = topic_prefix
        self._sent = 0
        self._delivered = 0

    def _on_delivery(self, err: Any, _msg: Any) -> None:
        if err is None:
            self._delivered += 1
        else:
            log.warning("kafka delivery failed: %s", err)

    def publish(self, topic: str, key: str, value: bytes) -> None:
        self._sent += 1
        for _ in range(3):
            try:
                self._producer.produce(
                    self._prefix + topic, value=value, key=key, on_delivery=self._on_delivery
                )
                break
            except BufferError:  # local queue full: let librdkafka drain, then try again
                self._producer.poll(1.0)
        else:
            log.warning("kafka queue full, dropped message for %s", topic)
        self._producer.poll(0)

    def flush(self, timeout_s: float) -> tuple[int, int]:
        self._producer.flush(timeout_s)
        delivered, sent = self._delivered, self._sent
        self._delivered = self._sent = 0
        return delivered, sent - delivered

    def close(self) -> None:
        self._producer.flush(30)
