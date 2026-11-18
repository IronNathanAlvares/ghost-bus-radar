"""Two small static GTFS versions shaped like NTA's, for the timetable tests.

Version 2 behaves like a real NTA re-upload: **every** route_id and trip_id is regenerated, even
for routes that did not change. On top of that: route 46A is renamed, route 39 is withdrawn,
route 7 is new, and stop S2 is renamed. Natural keys must see through the id churn.
"""

from __future__ import annotations

import csv
import io
import json
import zipfile
from datetime import UTC, datetime
from email.utils import format_datetime
from pathlib import Path

from ghostbus_core.archive_layout import STATIC_FILENAME, static_prefix
from ghostbus_ingest.archive import Archive

V1_PUBLISHED = datetime(2026, 11, 1, 22, 0, tzinfo=UTC)
V2_PUBLISHED = datetime(2026, 11, 14, 23, 9, 2, tzinfo=UTC)

AGENCY = [
    {
        "agency_id": a,
        "agency_name": n,
        "agency_url": "https://example.test",
        "agency_timezone": "Europe/London",
    }
    for a, n in [("1", "Dublin Bus"), ("IR", "Irish Rail"), ("10000", "LUAS")]
]
CALENDAR = [
    {
        "service_id": sid,
        **dict(
            zip(
                ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"),
                days,
                strict=True,
            )
        ),
        "start_date": "20261101",
        "end_date": "20270430",
    }
    for sid, days in [("WK", "1111100"), ("SUN", "0000001")]
]
CALENDAR_DATES = [{"service_id": "WK", "date": "20261225", "exception_type": "2"}]


def _stops(s2_name: str) -> list[dict[str, str]]:
    return [
        {
            "stop_id": "S1",
            "stop_code": "101",
            "stop_name": "O'Connell St",
            "stop_lat": "53.3498",
            "stop_lon": "-6.2603",
        },
        {
            "stop_id": "S2",
            "stop_code": "102",
            "stop_name": s2_name,
            "stop_lat": "53.3394",
            "stop_lon": "-6.2490",
        },
        {
            "stop_id": "S3",
            "stop_code": "103",
            "stop_name": "Donnybrook",
            "stop_lat": "53.3220",
            "stop_lon": "-6.2372",
        },
        {
            "stop_id": "S4",
            "stop_code": "104",
            "stop_name": "Heuston",
            "stop_lat": "53.3464",
            "stop_lon": "-6.2930",
        },
    ]


def _route(rid: str, agency: str, short: str, long: str, rtype: str) -> dict[str, str]:
    return {
        "route_id": rid,
        "agency_id": agency,
        "route_short_name": short,
        "route_long_name": long,
        "route_type": rtype,
    }


def _trip(
    rid: str, sid: str, tid: str, direction: str, block: str, times: list[tuple[str, str]]
) -> tuple[dict[str, str], list[dict[str, str]]]:
    trip = {
        "route_id": rid,
        "service_id": sid,
        "trip_id": tid,
        "trip_headsign": "",
        "direction_id": direction,
        "block_id": block,
        "shape_id": "",
    }
    st = [
        {
            "trip_id": tid,
            "arrival_time": t,
            "departure_time": t,
            "stop_id": stop,
            "stop_sequence": str(i + 1),
            "pickup_type": "0",
            "drop_off_type": "0",
            "timepoint": "1",
        }
        for i, (stop, t) in enumerate(times)
    ]
    return trip, st


def version(n: int) -> dict[str, list[dict[str, str]]]:
    p = f"v{n}"  # every id carries the version, like NTA's regenerated ids
    r46a, r39, rir, rluas, r7 = (f"{p}_46A", f"{p}_39", f"{p}_GALWAY", f"{p}_GREEN", f"{p}_7")
    routes = [
        _route(
            r46a,
            "1",
            "46A",
            "Phoenix Park - Dun Laoghaire" if n == 1 else "Phoenix Park - Dún Laoghaire",
            "3",
        ),
        _route(rir, "IR", "rail", "Dublin - Galway", "2"),
        _route(rluas, "10000", "Green", "Parnell - Brides Glen", "0"),
    ]
    specs = [
        (r46a, "WK", "0", "b1", [("S1", "08:00:00"), ("S2", "08:20:00"), ("S3", "08:40:00")]),
        (r46a, "WK", "0", "b2", [("S1", "24:30:00"), ("S3", "25:10:00")]),  # after midnight
        (rir, "WK", "0", "b3", [("S1", "10:00:00"), ("S4", "12:30:00")]),
        (rluas, "WK", "1", "b4", [("S2", "10:25:00"), ("S4", "11:10:00")]),
        (rluas, "WK", "1", "b5", [("S2", "10:25:00"), ("S4", "11:10:00")]),  # identical twin
        (r46a, "SUN", "0", "b6", [("S1", "08:15:00"), ("S3", "08:55:00")]),  # spring-forward Sunday
    ]
    if n == 1:
        routes.append(_route(r39, "1", "39", "Ongar - Burlington Road", "3"))
        specs.append((r39, "WK", "1", "b7", [("S3", "09:00:00"), ("S4", "09:30:00")]))
    else:
        routes.append(_route(r7, "1", "7", "Mountjoy Sq - Loughlinstown", "3"))
        specs.append((r7, "WK", "0", "b8", [("S4", "07:00:00"), ("S1", "07:45:00")]))
    trips, stop_times = [], []
    for i, (rid, sid, d, block, times) in enumerate(specs):
        t, st = _trip(rid, sid, f"{p}_{i}", d, block, times)
        trips.append(t)
        stop_times += st
    return {
        "agency.txt": AGENCY,
        "routes.txt": routes,
        "stops.txt": _stops("Merrion Sq" if n == 1 else "Merrion Square"),
        "calendar.txt": CALENDAR,
        "calendar_dates.txt": CALENDAR_DATES,
        "trips.txt": trips,
        "stop_times.txt": stop_times,
        "feed_info.txt": [
            {
                "feed_publisher_name": "Test",
                "feed_version": f"VERSION-{n}",
                "feed_start_date": "20261101",
                "feed_end_date": "20270430",
            }
        ],
    }


def to_zip(files: dict[str, list[dict[str, str]]]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, rows in files.items():
            text = io.StringIO()
            fields = list(dict.fromkeys(k for r in rows for k in r))
            w = csv.DictWriter(text, fieldnames=fields, lineterminator="\n")
            w.writeheader()
            w.writerows(rows)
            zf.writestr(name, text.getvalue())
    return buf.getvalue()


def archive_version(archive: Archive, n: int, published: datetime, tmp: Path) -> str:
    """Store version ``n`` exactly as the static watcher would. Returns its sha256."""
    return archive_files(archive, version(n), published, tmp)


def archive_files(
    archive: Archive, files: dict[str, list[dict[str, str]]], published: datetime, tmp: Path
) -> str:
    """Store any set of GTFS files as one archived upload. Returns its sha256."""
    import hashlib

    data = to_zip(files)
    n = hashlib.sha256(data).hexdigest()[:8]
    sha = hashlib.sha256(data).hexdigest()
    path = tmp / f"v{n}.zip"
    path.write_bytes(data)
    archive.put_file(static_prefix(sha) + STATIC_FILENAME, path, "application/zip")
    manifest = {
        "sha256": sha,
        "bytes": len(data),
        "fetched_at_ms": int(published.timestamp() * 1000) + 600_000,
        "last_modified": format_datetime(published, usegmt=True),
        "feed_info": files["feed_info.txt"][0],
    }
    archive.put_bytes(
        static_prefix(sha) + "manifest.json", json.dumps(manifest).encode(), "application/json"
    )
    return sha
