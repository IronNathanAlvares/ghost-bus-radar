"""Streaming metrics: one flat record per micro-batch of every query.

Fields: batch duration, input rows, rows written, **late rows** (dropped by the watermark),
state size, and **watermark lag** (how far event time trails the wall clock). Locally they are
logged and appended to a JSON-lines file next to the checkpoints; in AWS the same records go to
CloudWatch (Sprint 5).

``flatten`` is pure and unit-tested; the listener is a thin adapter around it.
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql.streaming.listener import StreamingQueryListener

log = logging.getLogger("ghostbus.stream.metrics")


def _ts(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def flatten(progress: dict[str, Any]) -> dict[str, Any]:
    """Spark's ``StreamingQueryProgress`` JSON to the flat record we keep."""
    duration = progress.get("durationMs") or {}
    event_time = progress.get("eventTime") or {}
    ops = progress.get("stateOperators") or []
    sink = progress.get("sink") or {}
    at = _ts(progress.get("timestamp"))
    watermark = _ts(event_time.get("watermark"))
    lag = (at - watermark).total_seconds() if at and watermark and watermark.year > 1970 else None
    return {
        "query": progress.get("name"),
        "batch_id": progress.get("batchId"),
        "timestamp": progress.get("timestamp"),
        "input_rows": progress.get("numInputRows", 0),
        "output_rows": sink.get("numOutputRows"),
        "batch_ms": duration.get("triggerExecution"),
        "add_batch_ms": duration.get("addBatch"),
        "watermark": event_time.get("watermark"),
        "max_event_time": event_time.get("max"),
        "watermark_lag_s": lag,
        "late_rows": sum(int(op.get("numRowsDroppedByWatermark", 0)) for op in ops),
        "state_rows": sum(int(op.get("numRowsTotal", 0)) for op in ops),
        "state_memory_bytes": sum(int(op.get("memoryUsedBytes", 0)) for op in ops),
    }


class MetricsListener(StreamingQueryListener):
    """Collects flattened progress records; optionally appends them to a JSONL file."""

    def __init__(self, path: Path | None = None) -> None:
        self.path = path
        self.records: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)

    def onQueryStarted(self, event: Any) -> None:
        log.info("query started: %s (%s)", event.name, event.id)

    def onQueryProgress(self, event: Any) -> None:
        rec = flatten(json.loads(event.progress.json))
        with self._lock:
            self.records.append(rec)
            if self.path is not None:
                with self.path.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(rec) + "\n")
        if rec["input_rows"] or rec["late_rows"]:
            log.info(
                "%s batch %s: %s rows in, %s out, %s late, %s ms, watermark lag %s s",
                rec["query"],
                rec["batch_id"],
                rec["input_rows"],
                rec["output_rows"],
                rec["late_rows"],
                rec["batch_ms"],
                None if rec["watermark_lag_s"] is None else round(rec["watermark_lag_s"]),
            )

    def onQueryIdle(self, event: Any) -> None:
        pass

    def onQueryTerminated(self, event: Any) -> None:
        if event.exception:
            log.error("query %s failed: %s", event.id, event.exception)

    def total(self, query: str, field: str) -> int:
        with self._lock:
            return sum(int(r.get(field) or 0) for r in self.records if r["query"] == query)
