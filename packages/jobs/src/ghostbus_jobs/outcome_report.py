"""Sprint 4 measures from ``gold.trip_outcomes``: rates with Wilson intervals, and how much the
headline rates move when the thresholds move (sensitivity).

Rates use only bus trips whose whole evidence window was collected (``in_rates``): chosen by
coverage, never by outcome, because a bus tracked to the end survives a collection gap while a
missing bus does not. Every rate comes with its sample size and a 95% Wilson interval. Routes
with fewer than 30 such trips are not ranked.

    docker compose -f local/docker-compose.yml run --rm spark \\
        python -m ghostbus_jobs.outcome_report --from 2026-11-15 --to 2026-11-15
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import Counter, defaultdict
from dataclasses import replace
from datetime import UTC, date
from typing import Any

from ghostbus_core.lake import LakeSettings
from ghostbus_core.outcomes import (
    DEFAULT,
    RATED,
    UNTRACKED,
    Evidence,
    Outcome,
    classify,
    in_rates,
    wilson,
)
from ghostbus_jobs.outcomes import GOLD
from ghostbus_jobs.spark import build_spark

AGENCY = {
    "1": "Dublin Bus",
    "2": "Bus Éireann",
    "3": "Go-Ahead Ireland",
    "03C": "Go-Ahead Ireland (commuter)",
    "WFRD": "Bus Éireann Waterford",
}
MIN_RANKED = 30
#: Counter key for bus trips left out of rates because a collection gap overlapped them.
GAP = "_gap"


def rate(k: int, n: int) -> str:
    if n == 0:
        return "-"
    lo, hi = wilson(k, n)
    return f"{100 * k / n:.1f}% ({100 * lo:.1f}-{100 * hi:.1f})"


def table(headers: list[str], rows: list[list[object]]) -> str:
    out = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    return "\n".join(out + ["| " + " | ".join(str(c) for c in r) + " |" for r in rows])


def rates_rows(groups: dict[str, Counter[str]]) -> list[list[object]]:
    rows = []
    for name, c in groups.items():
        n = sum(v for o, v in c.items() if o != GAP)
        untracked = sum(c[o.value] for o in UNTRACKED)
        rows.append(
            [
                name,
                n,
                rate(c["RAN"], n),
                rate(untracked, n),
                rate(c["VANISHED"], n),
                rate(c["CANCELLED_LATE"], n),
                rate(c["CANCELLED_EARLY"], n),
                c[GAP],
            ]
        )
    return sorted(rows, key=lambda r: -int(str(r[1])))


HEAD = [
    "",
    "Trips in rates",
    "Ran",
    "Untracked (ghost + phantom)",
    "Vanished",
    "Cancelled late",
    "Cancelled early",
    "Not in rates (collection gap)",
]


def evidence(r: dict[str, Any]) -> Evidence:
    return Evidence(
        route_type=r["route_type"],
        scheduled_start=r["scheduled_start"].replace(tzinfo=UTC),
        cancelled_at=r["cancelled_at"].replace(tzinfo=UTC) if r["cancelled_at"] else None,
        tu_snapshots=r["tu_snapshots"],
        tu_vehicle=r["tu_vehicle"],
        vp_positions=r["vp_positions"],
        progress=r["progress"],
        tu_gap=r["tu_gap"],
        vp_gap=r["vp_gap"],
    )


def sensitivity(rows: list[dict[str, Any]]) -> str:
    """Reclassify the stored evidence with other thresholds. (Stop radius and gap length change
    the evidence itself and need a reconcile run with other Params.)"""
    ev = [evidence(r) for r in rows]
    out = []
    for ran in (0.4, 0.5, 0.6, 0.7, 0.8):
        for lead in (5, 10, 15, 30):
            p = replace(DEFAULT, ran_progress=ran, cancel_lead_min=lead)
            c = Counter(classify(e, p)[0] for e in ev)
            n = sum(v for o, v in c.items() if o in RATED)
            mark = " (default)" if p == DEFAULT else ""
            out.append(
                [
                    f"{ran:.0%} / {lead} min{mark}",
                    n,
                    rate(sum(c[o] for o in UNTRACKED), n),
                    rate(c[Outcome.VANISHED], n),
                    rate(c[Outcome.CANCELLED_LATE], n),
                ]
            )
    return table(
        [
            "RAN threshold / early-cancel notice",
            "Trips in rates",
            "Untracked",
            "Vanished",
            "Cancelled late",
        ],
        out,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from", dest="first", type=date.fromisoformat, required=True)
    parser.add_argument("--to", dest="last", type=date.fromisoformat, required=True)
    parser.add_argument("--status", default="final")
    args = parser.parse_args(argv)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    spark = build_spark("outcome_report", LakeSettings.from_env(dict(os.environ)))
    spark.sparkContext.setLogLevel("ERROR")
    rows = [
        r.asDict()
        for r in spark.table(GOLD)
        .where(
            f"service_date BETWEEN DATE '{args.first}' AND DATE '{args.last}' "
            f"AND status = '{args.status}'"
        )
        .collect()
    ]
    totals = Counter(r["outcome"] for r in rows)
    print(f"## All outcomes, {args.first} to {args.last} ({len(rows):,} scheduled trips)\n")
    print(table(["Outcome", "Trips"], [[o.value, f"{totals[o.value]:,}"] for o in Outcome]))

    sample = [r for r in rows if in_rates(evidence(r))]
    through_gap = sum(1 for r in rows if r["outcome"] in RATED and not in_rates(evidence(r)))
    print(
        f"\nIn rates (bus trips, whole window collected): {len(sample):,}. "
        f"Decided through a collection gap, kept as outcomes but not in rates: {through_gap:,}."
    )
    by_agency: dict[str, Counter[str]] = defaultdict(Counter)
    by_route: dict[str, Counter[str]] = defaultdict(Counter)
    for r in rows:
        if r["outcome"] == "OUT_OF_SCOPE":
            continue
        key = r["outcome"] if in_rates(evidence(r)) else GAP
        by_agency[AGENCY.get(r["agency_id"], r["agency_id"])][key] += 1
        short = r["route_key"].split("|")[1]
        by_route[f"{AGENCY.get(r['agency_id'], r['agency_id'])} {short}"][key] += 1
    by_agency["All buses"] = sum(by_agency.values(), Counter())
    print("\n## Rates by operator (95% Wilson interval)\n")
    print(table(["Operator", *HEAD[1:]], rates_rows(by_agency)))
    ranked = {
        k: v for k, v in by_route.items() if sum(n for o, n in v.items() if o != GAP) >= MIN_RANKED
    }
    print(
        f"\n## Routes with at least {MIN_RANKED} trips in rates "
        f"({len(ranked)} of {len(by_route)})\n"
    )
    print(table(["Route", *HEAD[1:]], rates_rows(ranked)) if ranked else "None yet.")
    print("\n## Sensitivity: how the rates move with the thresholds\n")
    print(sensitivity(sample) if sample else "No trips in rates.")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
