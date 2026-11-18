"""Small, dependency-free reads of a static GTFS zip. Full parsing arrives in Sprint 2."""

from __future__ import annotations

import csv
import io
import zipfile
from pathlib import Path


def read_csv_member(zip_path: Path, member: str) -> list[dict[str, str]]:
    """Rows of one small text file inside the zip (``feed_info.txt``, ``routes.txt``, ...)."""
    with zipfile.ZipFile(zip_path) as zf, zf.open(member) as raw:
        text = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
        return [dict(row) for row in csv.DictReader(text)]


def read_feed_info(zip_path: Path) -> dict[str, str]:
    """The single ``feed_info.txt`` row. ``feed_version`` is the SCD2 version key (ADR-0002)."""
    rows = read_csv_member(zip_path, "feed_info.txt")
    if len(rows) != 1:
        raise ValueError(f"feed_info.txt has {len(rows)} rows, expected 1")
    return rows[0]
