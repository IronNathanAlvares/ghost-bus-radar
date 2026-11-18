"""One heartbeat per API call. The record of our own uptime.

Heartbeats decide the ``NO_DATA`` outcome: a trip whose window overlaps a gap in *our* collection
is excluded from every rate, so our downtime can never be counted as a ghost bus. They are written
to the raw archive (durable) and to Kafka (live monitoring).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

from ghostbus_core.feeds import Feed
from ghostbus_core.gtfsr import SCHEMA_VERSION, Json
from ghostbus_core.pacing import FetchStatus


@dataclass(frozen=True)
class Heartbeat:
    fetch_id: str
    feed: Feed
    fetched_at_ms: int
    status: FetchStatus
    http_status: int | None
    elapsed_ms: int
    bytes: int
    #: Header timestamp of the snapshot, epoch seconds. ``fetched_at - feed_timestamp`` is feed lag.
    feed_timestamp: int | None
    entity_count: int | None
    records: int
    entity_errors: int
    archive_key: str | None
    archived: bool
    kafka_delivered: int
    #: Messages not confirmed by Kafka when the poll ended (failed or still queued).
    kafka_unconfirmed: int
    error: str | None
    retry_after_s: float | None = None

    @property
    def collected(self) -> bool:
        """The snapshot is safe in the archive, so it can be replayed whatever Kafka did."""
        return self.status is FetchStatus.OK and self.archived

    def to_json(self) -> Json:
        d: Json = asdict(self)
        d["feed"] = self.feed.value
        d["status"] = self.status.value
        d["collected"] = self.collected
        d["schema_version"] = SCHEMA_VERSION
        return d
