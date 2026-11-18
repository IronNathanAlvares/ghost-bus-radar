"""Read archived static GTFS versions back: the manifests the watcher wrote, and the zips.

``valid_from`` of a version is its publish time (the ``Last-Modified`` NTA served it with), falling
back to when we first fetched it. Realtime ids switch to the new version at publication, so the
publish time is the better estimate; the unmatched-id rate measures how good it is.
"""

from __future__ import annotations

import json
import zipfile
from dataclasses import dataclass
from datetime import UTC, date, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path

from ghostbus_core.archive_layout import STATIC_FILENAME, static_prefix
from ghostbus_core.timetable import FeedVersion, order_versions, parse_gtfs_date, version_key
from ghostbus_ingest.archive import Archive


@dataclass(frozen=True)
class ArchivedVersion:
    version: FeedVersion
    manifest: dict[str, object]

    @property
    def zip_key(self) -> str:
        return static_prefix(self.version.sha256) + STATIC_FILENAME


def _published_at(manifest: dict[str, object]) -> datetime:
    last_modified = manifest.get("last_modified")
    if isinstance(last_modified, str) and last_modified:
        return parsedate_to_datetime(last_modified).astimezone(UTC)
    fetched = manifest["fetched_at_ms"]
    assert isinstance(fetched, int | float)
    return datetime.fromtimestamp(fetched / 1000, tz=UTC)


def _date(info: dict[str, str], name: str) -> date | None:
    value = info.get(name, "").strip()
    return parse_gtfs_date(value) if value else None


def list_versions(archive: Archive) -> list[ArchivedVersion]:
    """Every complete archived version, oldest first. A zip without a manifest is incomplete."""
    out = []
    for key in archive.list_keys("raw/feed=static/"):
        if not key.endswith("/manifest.json"):
            continue
        raw = archive.get_bytes(key)
        if raw is None:
            continue
        manifest = json.loads(raw)
        info = manifest.get("feed_info") or {}
        version = FeedVersion(
            feed_version=version_key(
                str(info.get("feed_version") or "unlabelled"), str(manifest["sha256"])
            ),
            sha256=str(manifest["sha256"]),
            valid_from=_published_at(manifest),
            feed_start_date=_date(info, "feed_start_date"),
            feed_end_date=_date(info, "feed_end_date"),
        )
        out.append(ArchivedVersion(version, manifest))
    ordered = order_versions(v.version for v in out)
    by_sha = {v.version.sha256: v for v in out}
    return [by_sha[v.sha256] for v in ordered]


def extract(archive: Archive, version: ArchivedVersion, dest: Path) -> Path:
    """Unzip one version's text files into ``dest/<sha256>/`` (idempotent) and return the dir."""
    target = dest / version.version.sha256
    done = target / ".complete"
    if done.exists():
        return target
    data = archive.get_bytes(version.zip_key)
    if data is None:
        raise FileNotFoundError(version.zip_key)
    target.mkdir(parents=True, exist_ok=True)
    zip_path = target / STATIC_FILENAME
    zip_path.write_bytes(data)
    with zipfile.ZipFile(zip_path) as zf:
        for member in zf.namelist():
            if member.endswith(".txt") and "/" not in member and member != "shapes.txt":
                zf.extract(member, target)
    zip_path.unlink()
    done.write_text("ok")
    return target
