"""Chaos test: SIGKILL the streaming pipeline 20 times mid-flight, then compare with a clean run.

The pipeline runs as a separate process with tiny micro-batches (so it is almost always in the
middle of a batch or a commit) and is killed with SIGKILL, the whole process group, JVM included:
no shutdown hooks, no graceful stop. After each kill the checkpoints are inspected: a batch that
has an ``offsets`` entry but no ``commits`` entry was in flight when the process died.

Then the pipeline is run to completion and every table is compared with an uninterrupted run over
the same input. Exactly-once means: identical rows, no duplicates, nothing missing.

    docker compose -f local/docker-compose.yml run --rm spark pytest -m chaos -s
"""

from __future__ import annotations

import json
import os
import random
import signal
import subprocess
import sys
import time
import uuid
from collections import Counter
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import SparkSession

from ghostbus_core.feeds import Feed
from ghostbus_core.lake import LakeSettings
from ghostbus_ingest.kafka_config import bootstrap_from_env
from ghostbus_jobs.spark import build_spark
from ghostbus_jobs.stream import QUERY_ORDER, SILVER_TABLES, StreamConfig, run_available_now
from tests.stream_data import Msg, make_topics, produce, snapshot, trip, vehicle

pytestmark = pytest.mark.chaos

BOOTSTRAP = bootstrap_from_env(os.environ)
KILLS = int(os.environ.get("CHAOS_KILLS", "20"))
RESULT = Path(os.environ.get("CHAOS_RESULT", "/checkpoints/chaos-result.json"))
T0 = 1_794_770_400


def dataset(cfg: StreamConfig, rng: random.Random) -> list[Msg]:
    """27 polls of each feed over 9 minutes, with stale vehicle repeats and replayed fetches.

    Every distinct event lies within one 10-minute watermark of every other, so no row is ever
    *late*, and the expected output does not depend on where micro-batches happen to split.
    """
    msgs: list[Msg] = []
    snaps: list[list[Msg]] = []
    for i in range(27):
        t = T0 + 20 * i
        trips = [trip(f"T{j}", f"trip{j}", rng.randint(1, 4)) for j in range(30)]
        snaps.append(snapshot(Feed.TRIP_UPDATES, cfg.topics, t, trips))
        # Each vehicle reports every 40 s, so every other snapshot repeats a stale position.
        vehicles = [vehicle(f"V{j}", f"bus{j}", f"trip{j}", t - (t - j) % 40) for j in range(20)]
        snaps.append(snapshot(Feed.VEHICLES, cfg.topics, t + 10, vehicles))
    for s in snaps:
        msgs += s
    for k in rng.sample(range(len(snaps)), 5):
        msgs += snaps[k]  # replays
    return msgs


def run_pipeline_process(cfg: StreamConfig) -> subprocess.Popen[bytes]:
    cmd = [
        sys.executable,
        "-m",
        "ghostbus_jobs.stream",
        "--trigger",
        "0 seconds",
        "--bronze-ns",
        cfg.bronze_ns,
        "--silver-ns",
        cfg.silver_ns,
        "--topics",
        json.dumps(cfg.topics.__dict__),
        "--checkpoint-root",
        str(cfg.checkpoint_root),
        "--max-offsets-per-trigger",
        "15",
        "--max-files-per-batch",
        "1",
        "--fail-on-data-loss",
    ]
    return subprocess.Popen(
        cmd, start_new_session=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )


def in_flight(cfg: StreamConfig) -> list[str]:
    """Queries whose latest batch was planned (offsets written) but never committed."""
    out = []
    for q in QUERY_ORDER:
        root = cfg.checkpoint_root / q

        def ids(sub: str, root: Path = root) -> list[int]:
            d = root / sub
            return [int(p.name) for p in d.iterdir() if p.name.isdigit()] if d.exists() else []

        offsets, commits = ids("offsets"), ids("commits")
        if offsets and max(offsets) > max(commits, default=-1):
            out.append(q)
    return out


def table_rows(spark: SparkSession, table: str, drop: tuple[str, ...] = ()) -> Counter[str]:
    spark.sql(f"REFRESH TABLE {table}")
    df = spark.table(table).drop(*drop)
    return Counter(
        json.dumps(r.asDict(recursive=True), sort_keys=True, default=str) for r in df.collect()
    )


def orphan_files(spark: SparkSession, table: str) -> int:
    """Data files in the table's location that no snapshot references: debris of killed writes."""
    import boto3

    location = next(
        r["data_type"]
        for r in spark.sql(f"DESCRIBE TABLE EXTENDED {table}").collect()
        if r["col_name"] == "Location"
    )
    bucket, prefix = location.removeprefix("s3://").split("/", 1)
    s3 = boto3.client("s3", endpoint_url=os.environ["LAKE_S3_ENDPOINT"])
    stored = {
        f"s3://{bucket}/{o['Key']}"
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=f"{prefix}/data/"
        )
        for o in page.get("Contents", [])
        if o["Key"].endswith(".parquet")
    }
    referenced = {
        r["file_path"] for r in spark.sql(f"SELECT file_path FROM {table}.all_data_files").collect()
    }
    return len(stored - referenced)


def finish(spark: SparkSession, cfg: StreamConfig) -> None:
    """Restart and run to completion, as a supervisor would. Idempotent, so retrying is safe."""
    for attempt in range(3):
        try:
            run_available_now(spark, cfg, BOOTSTRAP)
            return
        except Exception:
            if attempt == 2:
                raise
            time.sleep(2)


def test_exactly_once_under_sigkill(tmp_path: Path) -> None:
    rng = random.Random(20261115)
    spark = build_spark("chaos", LakeSettings.from_env(dict(os.environ)))
    tag = uuid.uuid4().hex[:6]
    topics = make_topics(BOOTSTRAP, partitions=3)
    base = Path("/checkpoints") / f"chaos-{tag}"
    clean = StreamConfig(
        bronze_ns=f"lake.chaos_{tag}_clean_b",
        silver_ns=f"lake.chaos_{tag}_clean_s",
        topics=topics,
        checkpoint_root=base / "clean",
        fail_on_data_loss=True,
    )
    chaos = StreamConfig(
        bronze_ns=f"lake.chaos_{tag}_b",
        silver_ns=f"lake.chaos_{tag}_s",
        topics=topics,
        checkpoint_root=base / "chaos",
        fail_on_data_loss=True,
    )
    msgs = dataset(clean, rng)
    produce(BOOTSTRAP, msgs)

    finish(spark, clean)

    kills: list[dict[str, Any]] = []
    for i in range(KILLS):
        proc = run_pipeline_process(chaos)
        delay = rng.uniform(12, 30)
        time.sleep(delay)
        alive = proc.poll() is None
        os.killpg(proc.pid, signal.SIGKILL)
        proc.wait()
        flying = in_flight(chaos)
        kills.append(
            {"kill": i + 1, "after_s": round(delay, 1), "was_running": alive, "in_flight": flying}
        )
        print(f"kill {i + 1}/{KILLS} after {delay:.1f}s: in flight {flying or '-'}", flush=True)

    finish(spark, chaos)

    tables = [("bronze", clean.bronze, chaos.bronze, ("ingested_at",))] + [
        (t, clean.silver(t), chaos.silver(t), ()) for t in SILVER_TABLES
    ]
    report: dict[str, Any] = {"messages": len(msgs), "kills": kills, "tables": {}}
    for name, a, b, drop in tables:
        expected, actual = table_rows(spark, a, drop), table_rows(spark, b, drop)
        report["tables"][name] = {
            "clean_rows": sum(expected.values()),
            "chaos_rows": sum(actual.values()),
            "duplicates": sum(c - 1 for c in actual.values() if c > 1),
            "missing": sum((expected - actual).values()),
            "extra": sum((actual - expected).values()),
            "orphan_files": orphan_files(spark, b),
        }
    report["kills_mid_batch"] = sum(1 for k in kills if k["in_flight"])
    RESULT.write_text(json.dumps(report, indent=2))
    print(
        json.dumps(report["tables"], indent=2),
        f"\nkills mid-batch: {report['kills_mid_batch']}/{KILLS}",
    )

    bronze_keys = Counter(
        (r["topic"], r["kafka_partition"], r["kafka_offset"])
        for r in spark.table(chaos.bronze).collect()
    )
    assert len(bronze_keys) == len(msgs) and max(bronze_keys.values()) == 1
    for name, t in report["tables"].items():
        assert t["duplicates"] == 0 and t["missing"] == 0 and t["extra"] == 0, name
    assert report["kills_mid_batch"] >= KILLS // 2, "too few kills landed mid-batch to mean much"
