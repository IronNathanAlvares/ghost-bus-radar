"""The public data export: the JSON files the website reads (roadmap 2.1). Pure.

The website needs no running compute: every night the export job writes small, deterministic JSON
documents under ``data/v1/``, and the site only reads them. Everything here is built from one list
of :class:`Cell` (counts per service day, route, local hour), so it is fully testable.

Publishing rules, all enforced here:

- **Only days that passed the quality gates count.** A day whose ``gold.data_quality`` overall
  status is ``fail`` is listed, with its status, but excluded from every rate (ADR-0011).
- **Rates over fully collected bus trips only** (``in_rates``, ADR-0008), always with the sample
  size and a 95% Wilson interval.
- **Routes are ranked only with at least 30 trips, and by the lower end of their interval**, so
  a route is called "worst" only when the data is confident about it.
- **NTA attribution** on every document (CC BY 4.0; ``docs/02-nta-usage-policy.md``).
- **Deterministic bytes**: sorted keys, fixed rounding. The same data gives the same file, so
  unchanged files are not re-uploaded and caches stay valid.
"""

from __future__ import annotations

import json
import re
import unicodedata
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from ghostbus_core.outcomes import wilson

SCHEMA_VERSION = "v1"
MIN_RANKED = 30
DIGITS = 4
ATTRIBUTION = {
    "source": "National Transport Authority (NTA), GTFS and GTFS-Realtime",
    "source_url": "https://developer.nationaltransport.ie/",
    "licence": "CC BY 4.0",
    "disclaimer": (
        "NTA data is provided as is; the NTA is not responsible for errors in it or in this "
        "analysis. Figures are derived by Ghost Bus Radar."
    ),
}
DAY_TYPES = ("weekday", "saturday", "sunday")
OUTCOMES = ("ran", "ghost", "phantom", "vanished", "cancelled_early", "cancelled_late")


@dataclass(frozen=True)
class Cell:
    """Bus trips in rates for one service day, route and local departure hour."""

    service_date: date
    agency: str
    route: str
    hour: int
    n: int
    ran: int = 0
    ghost: int = 0
    phantom: int = 0
    vanished: int = 0
    cancelled_early: int = 0
    cancelled_late: int = 0


@dataclass(frozen=True)
class Stop:
    code: str
    name: str
    lat: float
    lon: float
    routes: tuple[str, ...]


def day_type(d: date) -> str:
    """Timetables differ by day type. (Bank holidays run a Sunday-like service but are counted by
    their weekday here; the quality gate's week-away check flags them.)"""
    return {5: "saturday", 6: "sunday"}.get(d.weekday(), "weekday")


def slug(agency: str, route: str) -> str:
    """A stable, URL-safe file name for a route, e.g. ``dublin-bus-46a``. Accents are
    transliterated, not dropped: "Bus Éireann" becomes ``bus-eireann``, not ``bus-ireann``."""
    decomposed = unicodedata.normalize("NFKD", f"{agency}-{route}")
    text = "".join(ch for ch in decomposed if not unicodedata.combining(ch)).lower()
    return re.sub(r"[^a-z0-9]+", "-", text).strip("-")


def rate(k: int, n: int) -> dict[str, Any] | None:
    if n <= 0:
        return None
    lo, hi = wilson(k, n)
    return {
        "k": k,
        "n": n,
        "rate": round(k / n, DIGITS),
        "lo": round(lo, DIGITS),
        "hi": round(hi, DIGITS),
    }


@dataclass
class Counts:
    n: int = 0
    ran: int = 0
    ghost: int = 0
    phantom: int = 0
    vanished: int = 0
    cancelled_early: int = 0
    cancelled_late: int = 0

    def add(self, c: Cell) -> None:
        self.n += c.n
        for o in OUTCOMES:
            setattr(self, o, getattr(self, o) + getattr(c, o))

    def rates(self) -> dict[str, Any]:
        return {
            "trips_in_rates": self.n,
            "ran": rate(self.ran, self.n),
            "untracked": rate(self.ghost + self.phantom, self.n),
            "vanished": rate(self.vanished, self.n),
            "cancelled_early": rate(self.cancelled_early, self.n),
            "cancelled_late": rate(self.cancelled_late, self.n),
        }


def _sum(cells: Iterable[Cell]) -> Counts:
    out = Counts()
    for c in cells:
        out.add(c)
    return out


def publishable(quality: Mapping[date, str]) -> set[date]:
    """Days the quality gates let us publish. A day with no quality result is not published."""
    return {d for d, status in quality.items() if status != "fail"}


def _base(kind: str) -> dict[str, Any]:
    return {"schema": f"ghostbus.{kind}.{SCHEMA_VERSION}", "attribution": ATTRIBUTION}


def day_doc(day: date, cells: list[Cell], quality: Mapping[date, str]) -> dict[str, Any]:
    status = quality.get(day, "missing")
    ok = day in publishable(quality)
    todays = [c for c in cells if c.service_date == day]
    by_agency: dict[str, Counts] = defaultdict(Counts)
    for c in todays:
        by_agency[c.agency].add(c)
    doc = _base("day") | {
        "date": day.isoformat(),
        "day_type": day_type(day),
        "quality": status,
        "published": ok,
    }
    if not ok:
        doc["note"] = (
            "Not counted in any published rate: this day did not pass the data-quality gates "
            "(for example, too little of it was collected)."
        )
        return doc
    return (
        doc
        | _sum(todays).rates()
        | {
            "operators": [
                {"agency": a} | by_agency[a].rates()
                for a in sorted(by_agency, key=lambda a: -by_agency[a].n)
            ]
        }
    )


def league_doc(
    cells: list[Cell], quality: Mapping[date, str], end: date, days: int
) -> dict[str, Any]:
    start = end - timedelta(days=days - 1)
    window = sorted(d for d in publishable(quality) if start <= d <= end)
    chosen = set(window)
    by_route: dict[tuple[str, str], Counts] = defaultdict(Counts)
    for c in cells:
        if c.service_date in chosen:
            by_route[(c.agency, c.route)].add(c)
    ranked: list[dict[str, Any]] = []
    unranked: list[dict[str, Any]] = []
    for (agency, route), k in by_route.items():
        row = {"agency": agency, "route": route, "slug": slug(agency, route)} | k.rates()
        (ranked if k.n >= MIN_RANKED else unranked).append(row)
    # Worst first by the lower end of the untracked interval: confidently bad, not just noisy.
    ranked.sort(key=lambda r: (-r["untracked"]["lo"], -r["untracked"]["rate"], r["slug"]))
    for i, r in enumerate(ranked, start=1):
        r["rank"] = i
    unranked.sort(key=lambda r: r["slug"])
    return _base("league") | {
        "window_days": days,
        "from": start.isoformat(),
        "to": end.isoformat(),
        "days_published": [d.isoformat() for d in window],
        "min_trips_to_rank": MIN_RANKED,
        "ranked_by": "lower end of the 95% interval of the untracked rate",
        "routes": ranked,
        "not_ranked": unranked,
    }


def route_doc(
    agency: str, route: str, cells: list[Cell], quality: Mapping[date, str], end: date
) -> dict[str, Any]:
    ok = publishable(quality)
    mine = [c for c in cells if c.agency == agency and c.route == route and c.service_date in ok]
    daily: dict[date, Counts] = defaultdict(Counts)
    heat: dict[tuple[str, int], Counts] = defaultdict(Counts)
    for c in mine:
        daily[c.service_date].add(c)
        heat[(day_type(c.service_date), c.hour)].add(c)
    return _base("route") | {
        "agency": agency,
        "route": route,
        "slug": slug(agency, route),
        "to": end.isoformat(),
        "total": _sum(mine).rates(),
        "daily": [{"date": d.isoformat()} | daily[d].rates() for d in sorted(daily)],
        "by_hour": [
            {"day_type": t, "hour": h} | heat[(t, h)].rates()
            for t in DAY_TYPES
            for h in range(24)
            if (t, h) in heat
        ],
    }


def stops_doc(stops: Iterable[Stop]) -> dict[str, Any]:
    return _base("stops") | {
        "stops": [
            {
                "code": s.code,
                "name": s.name,
                "lat": round(s.lat, 5),
                "lon": round(s.lon, 5),
                "routes": sorted(set(s.routes)),
            }
            for s in sorted(stops, key=lambda s: s.code)
        ]
    }


def meta_doc(
    quality: Mapping[date, str],
    end: date,
    generated_at: str,
    params_version: str,
    chain_head: str | None,
    files: Iterable[str],
) -> dict[str, Any]:
    ok = sorted(publishable(quality))
    return _base("meta") | {
        "generated_at": generated_at,
        "to": end.isoformat(),
        "latest_published_day": ok[-1].isoformat() if ok else None,
        "days": [{"date": d.isoformat(), "quality": quality[d]} for d in sorted(quality)],
        "params_version": params_version,
        "archive_chain_head": chain_head,
        "files": sorted(files),
    }


def build(
    cells: list[Cell],
    quality: Mapping[date, str],
    stops: Iterable[Stop],
    end: date,
    generated_at: str,
    params_version: str,
    chain_head: str | None,
) -> dict[str, dict[str, Any]]:
    """Every document, keyed by its path under ``data/v1/``."""
    docs: dict[str, dict[str, Any]] = {}
    for d in sorted(quality):
        docs[f"days/{d.isoformat()}.json"] = day_doc(d, cells, quality)
    for n in (7, 28):
        docs[f"league/{n}d.json"] = league_doc(cells, quality, end, n)
    routes = sorted({(c.agency, c.route) for c in cells if c.service_date in publishable(quality)})
    for agency, route in routes:
        docs[f"routes/{slug(agency, route)}.json"] = route_doc(agency, route, cells, quality, end)
    docs["stops.json"] = stops_doc(stops)
    # generated_at is volatile, so meta.json is the one file that changes every night.
    docs["meta.json"] = meta_doc(quality, end, generated_at, params_version, chain_head, docs)
    return docs


def dumps(doc: Mapping[str, Any]) -> bytes:
    """Deterministic bytes: the same document always serialises identically."""
    return json.dumps(doc, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )
