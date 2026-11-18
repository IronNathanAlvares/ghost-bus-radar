"""Create every topic in ``ghostbus_core.topics`` if it does not exist.

Usage::

    python -m ghostbus_ingest.kafka_admin            # uses $KAFKA_BOOTSTRAP
    python -m ghostbus_ingest.kafka_admin --replication-factor 2   # MSK
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic

from ghostbus_core.topics import Topic, all_topics
from ghostbus_ingest.kafka_config import admin_config, bootstrap_from_env

log = logging.getLogger(__name__)


def ensure_topics(
    bootstrap: str, replication_factor: int, timeout_s: float = 30.0, prefix: str = ""
) -> list[str]:
    """Create missing topics and return their names. Existing topics are left alone.

    ``prefix`` creates prefixed copies instead (see ``KafkaPublisher(topic_prefix=...)``).
    """
    admin = AdminClient(admin_config(bootstrap))
    existing = set(admin.list_topics(timeout=timeout_s).topics)
    wanted: list[Topic] = [t for t in all_topics() if prefix + t.name not in existing]
    if not wanted:
        return []
    futures = admin.create_topics(
        [
            NewTopic(
                prefix + t.name,
                num_partitions=t.partitions,
                replication_factor=replication_factor,
                config={"retention.ms": str(t.retention_ms), "compression.type": "producer"},
            )
            for t in wanted
        ],
        operation_timeout=timeout_s,
    )
    created = []
    for name, future in futures.items():
        future.result()  # raises on failure
        created.append(name)
        log.info("created topic %s", name)
    return sorted(created)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default=bootstrap_from_env(os.environ))
    parser.add_argument("--replication-factor", type=int, default=1)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    created = ensure_topics(args.bootstrap, args.replication_factor)
    print(f"topics created: {len(created)}; total managed: {len(all_topics())}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
