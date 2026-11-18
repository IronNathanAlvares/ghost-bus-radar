"""Compare field observations (docs/04-field-check.md) with computed trip outcomes.

python -m ghostbus_jobs.field_check /field_check/observations-2026-11-18.csv
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import Counter
from datetime import date, datetime, time
from pathlib import Path

from pyspark.sql import functions as F

from ghostbus_core.lake import LakeSettings
from ghostbus_core.outcomes import UNTRACKED, wilson
from ghostbus_core.timetable import LOCAL_TZ, gtfs_time_to_utc
from ghostbus_jobs.outcomes import GOLD
from ghostbus_jobs.spark import build_spark
from ghostbus_jobs.timetable_load import DIM

MATCH_MINUTES = 3
DID_NOT_COME = {o.value for o in UNTRACKED} | {"VANISHED", "CANCELLED_EARLY", "CANCELLED_LATE"}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv", type=Path)
    args = parser.parse_args(argv)
    obs = [
        r
        for r in csv.DictReader(args.csv.open(encoding="utf-8-sig"))
        if r["observed"].strip() in {"arrived", "no_show"} and "example" not in r.get("notes", "")
    ]
    spark = build_spark("field_check", LakeSettings.from_env(dict(os.environ)))
    spark.sparkContext.setLogLevel("ERROR")
    stops = spark.table(DIM.stop).where("is_current").select("stop_id", "stop_code")
    gold = spark.table(GOLD).select("service_date", "trip_instance_key", "outcome", "reason")
    inst = spark.table(DIM.trip_instance).select(
        "service_date", "trip_instance_key", "feed_version", "trip_id", "route_key"
    )

    results = []
    for o in obs:
        day = date.fromisoformat(o["date"].strip())
        local = datetime.combine(
            day, time.fromisoformat(o["scheduled_time"].strip()), tzinfo=LOCAL_TZ
        )
        candidates = (
            inst.where(F.col("service_date") == F.lit(day))
            .where(F.split("route_key", r"\|")[1] == o["route"].strip())
            .join(spark.table(DIM.stop_time), ["feed_version", "trip_id"])
            .join(stops.where(F.col("stop_code") == o["stop_code"].strip()), "stop_id")
            .join(gold, ["service_date", "trip_instance_key"], "left")
            .select("trip_instance_key", "departure_s", "outcome", "reason")
            .collect()
        )
        best = None
        for c in candidates:
            when = gtfs_time_to_utc(day, c["departure_s"]).astimezone(LOCAL_TZ)
            off = abs((when - local).total_seconds()) / 60
            if off <= MATCH_MINUTES and (best is None or off < best[0]):
                best = (off, c)
        results.append((o, best[1] if best else None))

    matched = [(o, c) for o, c in results if c is not None and c["outcome"]]
    print(f"Observations: {len(obs)}; matched to a decided trip: {len(matched)}\n")
    pairs = Counter((o["observed"], c["outcome"]) for o, c in matched)
    print("| You saw | We said | Count |\n|---|---|---|")
    for (seen, said), n in sorted(pairs.items()):
        print(f"| {seen} | {said} | {n} |")
    agree = sum(
        n
        for (seen, said), n in pairs.items()
        if (seen == "arrived" and said == "RAN") or (seen == "no_show" and said in DID_NOT_COME)
    )
    rated = sum(n for (_, said), n in pairs.items() if said not in {"NO_DATA", "OUT_OF_SCOPE"})
    lo, hi = wilson(agree, rated)
    print(
        f"\nAgreement: {agree}/{rated} ({100 * agree / max(rated, 1):.0f}%, "
        f"95% CI {100 * lo:.0f}-{100 * hi:.0f}%)"
    )
    untracked = [(o, c) for o, c in matched if c["outcome"] in {u.value for u in UNTRACKED}]
    came = sum(1 for o, _ in untracked if o["observed"] == "arrived")
    lo, hi = wilson(came, len(untracked))
    print(
        f"Called untracked but you saw it arrive: {came}/{len(untracked)} "
        f"(95% CI {100 * lo:.0f}-{100 * hi:.0f}%)"
    )
    missed = sum(1 for o, c in matched if o["observed"] == "no_show" and c["outcome"] == "RAN")
    print(f"Called RAN but you saw no bus: {missed}")
    unmatched = [o for o, c in results if c is None]
    if unmatched:
        print(
            f"\nNot matched to a scheduled trip ({len(unmatched)}): check stop code, route and time"
        )
        for o in unmatched[:10]:
            print(
                f"- {o['date']} stop {o['stop_code']} route {o['route']} at {o['scheduled_time']}"
            )
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
