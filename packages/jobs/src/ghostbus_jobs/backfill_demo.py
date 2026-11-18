"""Outage and backfill demo: what an outage does, and proof that backfill repairs it.

Over the same closed trips, compute outcomes three ways:

A. **Uninterrupted:** from the streaming silver tables.
B. **Outage:** the same, with every silver row (positions, trip updates, heartbeats) fetched
   inside ``--hole`` removed, as if Kafka and the stream had both been down and the data lost.
C. **Backfill:** from the raw archive only (``reconcile``'s path), which the collector kept
   writing throughout.

Claims checked: C equals A row for row (backfill = a run that never stopped), and B turns the
affected trips into NO_DATA, never into ghosts (our outage is never blamed on the bus).

    docker compose -f local/docker-compose.yml run --rm spark \\
        python -m ghostbus_jobs.backfill_demo --day 2026-11-15 --hole 20:30 21:30
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import Counter
from dataclasses import replace
from datetime import UTC, date, datetime, time
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ghostbus_core.lake import LakeSettings
from ghostbus_core.outcomes import UNTRACKED, Outcome
from ghostbus_jobs.outcomes import Sources, outcomes, sources_from_archive, sources_from_silver
from ghostbus_jobs.reconcile import data_clock, data_window
from ghostbus_jobs.spark import build_spark
from ghostbus_jobs.stream import StreamConfig

FALSE_ALARMS = {o.value for o in UNTRACKED | {Outcome.VANISHED}}
#: Compared between runs: the outcome (first), its reason and every piece of evidence behind it.
COMPARED = (
    "outcome",
    "reason",
    "tu_snapshots",
    "cancelled_at",
    "tu_vehicle",
    "vp_positions",
    "first_vp",
    "last_vp",
    "progress",
    "tu_gap",
    "vp_gap",
)


def punch(src: Sources, h0: datetime, h1: datetime) -> Sources:
    lo, hi = F.lit(h0.replace(tzinfo=None)), F.lit(h1.replace(tzinfo=None))

    def cut(df: DataFrame) -> DataFrame:
        return df.where(~F.col("fetched_at").between(lo, hi))

    return replace(
        src,
        trip_updates=cut(src.trip_updates),
        vehicle_positions=cut(src.vehicle_positions),
        heartbeats=cut(src.heartbeats),
        name="silver-with-outage",
    )


def collect(df: DataFrame) -> dict[str, tuple[Any, ...]]:
    rows = df.select("trip_instance_key", *COMPARED).collect()
    return {r["trip_instance_key"]: tuple(r[c] for c in COMPARED) for r in rows}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", type=date.fromisoformat, required=True)
    parser.add_argument("--hole", nargs=2, metavar=("HH:MM", "HH:MM"), required=True, help="UTC")
    args = parser.parse_args(argv)
    spark: SparkSession = build_spark("backfill_demo", LakeSettings.from_env(dict(os.environ)))
    spark.sparkContext.setLogLevel("ERROR")

    lo, hi = data_window(args.day, args.day)
    h0, h1 = (datetime.combine(args.day, time.fromisoformat(t), tzinfo=UTC) for t in args.hole)
    silver = sources_from_silver(spark, StreamConfig(), lo, hi)
    archive = sources_from_archive(
        spark, os.environ["ARCHIVE_URI"], os.environ.get("ARCHIVE_S3_ENDPOINT"), lo, hi
    )
    clocks = [c for c in (data_clock(silver), data_clock(archive)) if c is not None]
    clock = min(clocks)  # judge all three on exactly the same closed trips

    def run(src: Sources) -> dict[str, tuple[Any, ...]]:
        return collect(outcomes(spark, src, args.day, args.day, clock, "demo", horizon_start=lo))

    a, b, c = run(silver), run(punch(silver, h0, h1)), run(archive)
    keys = sorted(a)
    diff_ac = [k for k in keys if a[k] != c.get(k)]
    changed_b = [k for k in keys if a[k][0] != b[k][0]]
    false_alarms = [k for k in changed_b if b[k][0] in FALSE_ALARMS]

    print(f"## Outage {h0:%H:%M} to {h1:%H:%M} UTC on {args.day}, data clock {clock:%H:%M} UTC\n")
    print(
        f"Trips decided: {len(keys):,} (same set in all three runs: {set(a) == set(b) == set(c)})\n"
    )
    print("| Outcome | A uninterrupted | B with outage | C backfill from archive |")
    print("|---|---|---|---|")
    ca, cb, cc = (Counter(row[0] for row in x.values()) for x in (a, b, c))
    for o in Outcome:
        print(f"| {o.value} | {ca[o.value]:,} | {cb[o.value]:,} | {cc[o.value]:,} |")
    print(
        "\n- Rows where backfill (C) differs from uninterrupted (A) in outcome, reason or any "
        f"evidence column: **{len(diff_ac)}**"
    )
    for k in diff_ac[:10]:
        print(f"  - `{k}`: A {a[k]}; C {c.get(k)}")
    print(
        f"- Trips the outage changed (B vs A): {len(changed_b):,}, of which NO_DATA: "
        f"{sum(1 for k in changed_b if b[k][0] == 'NO_DATA'):,}"
    )
    print(
        f"- Trips the outage wrongly turned into ghosts or vanished buses: **{len(false_alarms)}**"
    )
    spark.stop()
    return 0 if not diff_ac and not false_alarms else 1


if __name__ == "__main__":
    sys.exit(main())
