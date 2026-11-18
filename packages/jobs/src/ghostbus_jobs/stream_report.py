"""Sprint 3 measure: event-to-silver latency on live data, plus the streaming metrics.

Latency of a row = commit time of the Iceberg snapshot that added it (from the table's own
``entries`` and ``snapshots`` metadata) minus the time the poller fetched it. Only rows fetched
after ``--since`` count, so the backlog replayed when the stream first starts is excluded.

    docker compose -f local/docker-compose.yml run --rm spark \\
        python -m ghostbus_jobs.stream_report --since 2026-11-15T21:00:00
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from ghostbus_core.lake import LakeSettings
from ghostbus_jobs.spark import build_spark
from ghostbus_jobs.stream import StreamConfig


def pct(values: list[float], q: float) -> float:
    s = sorted(values)
    return s[min(len(s) - 1, round(q * (len(s) - 1)))] if s else float("nan")


def latencies(spark: SparkSession, table: str, time_col: str, since: str) -> list[float]:
    rows = spark.sql(f"""
        SELECT unix_millis(s.committed_at) - unix_millis(r.{time_col}) AS ms
        FROM (SELECT {time_col}, _file FROM {table} WHERE {time_col} >= TIMESTAMP '{since}') r
        JOIN (SELECT snapshot_id, data_file.file_path AS file_path
              FROM {table}.entries WHERE status = 1) e ON r._file = e.file_path
        JOIN {table}.snapshots s ON s.snapshot_id = e.snapshot_id
    """).collect()
    return [r["ms"] / 1000 for r in rows]


def metrics_summary(path: Path, since: str) -> list[list[object]]:
    if not path.exists():
        return []
    recs = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    recs = [r for r in recs if (r.get("timestamp") or "") >= since and r.get("input_rows")]
    out = []
    for q in sorted({r["query"] for r in recs}):
        rs = [r for r in recs if r["query"] == q]
        batch = [float(r["batch_ms"]) / 1000 for r in rs if r.get("batch_ms") is not None]
        lag = [float(r["watermark_lag_s"]) for r in rs if r.get("watermark_lag_s") is not None]
        out.append(
            [
                q,
                len(rs),
                f"{sum(int(r['input_rows']) for r in rs):,}",
                f"{statistics.median(batch):.1f} / {pct(batch, 0.95):.1f}" if batch else "-",
                f"{sum(int(r['late_rows']) for r in rs):,}",
                f"{statistics.median(lag):.0f}" if lag else "-",
            ]
        )
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", required=True, help="UTC, e.g. 2026-11-15T21:00:00")
    parser.add_argument("--metrics", type=Path, default=Path("/checkpoints/stream/metrics.jsonl"))
    args = parser.parse_args(argv)
    since = args.since.replace("T", " ").rstrip("Z")
    spark = build_spark("stream_report", LakeSettings.from_env(dict(os.environ)))
    spark.sparkContext.setLogLevel("ERROR")
    cfg = StreamConfig()

    rows = []
    for label, table, col in [
        ("bronze (all topics)", cfg.bronze, "fetched_at"),
        ("silver trip_updates", cfg.silver("trip_updates"), "fetched_at"),
        ("silver vehicle_positions", cfg.silver("vehicle_positions"), "fetched_at"),
        ("silver heartbeats", cfg.silver("heartbeats"), "fetched_at"),
    ]:
        lat = latencies(spark, table, col, since)
        rows.append(
            [
                label,
                f"{len(lat):,}",
                f"{pct(lat, 0.5):.1f}",
                f"{pct(lat, 0.95):.1f}",
                f"{max(lat, default=float('nan')):.1f}",
            ]
        )
    print("## Fetch to queryable, seconds\n")
    print("| Table | Rows | p50 | p95 | max |\n|---|---|---|---|---|")
    for r in rows:
        print("| " + " | ".join(str(c) for c in r) + " |")
    print("\n## Streaming metrics (batches with input)\n")
    print(
        "| Query | Batches | Input rows | Batch s p50 / p95 | Late rows | Watermark lag s (p50) |"
    )
    print("|---|---|---|---|---|---|")
    for m in metrics_summary(args.metrics, args.since):
        print("| " + " | ".join(str(c) for c in m) + " |")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
