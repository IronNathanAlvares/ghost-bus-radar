"""Build what the Glue batch jobs need: our three wheels and the pinned third-party list.

    uv run python infra/glue/build_jobs.py      # -> infra/glue/dist/*.whl, modules.txt

Glue 5.1 already has Spark 3.5.6, Python 3.11, Iceberg 1.10 and boto3. The job installs our
wheels plus exactly the third-party modules below with ``--no-deps``, so pip can never pull a
second PySpark (a dependency of ``ghostbus-jobs`` for local use). Versions come from uv.lock, so
the cloud runs what the tests ran.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DIST = Path(__file__).resolve().parent / "dist"
PACKAGES = ["ghostbus-core", "ghostbus-ingest", "ghostbus-jobs"]
#: Imported by the jobs (zstandard, tzdata) or lazily by executors decoding snapshots (protobuf).
THIRD_PARTY = ["zstandard", "tzdata", "gtfs-realtime-bindings", "protobuf"]


def locked_versions() -> dict[str, str]:
    lock = (ROOT / "uv.lock").read_text(encoding="utf-8")
    pairs = re.findall(r'\[\[package\]\]\nname = "([^"]+)"\nversion = "([^"]+)"', lock)
    return dict(pairs)


def build() -> list[str]:
    shutil.rmtree(DIST, ignore_errors=True)
    DIST.mkdir(parents=True)
    for pkg in PACKAGES:
        subprocess.run(
            ["uv", "build", "--package", pkg, "--wheel", "--out-dir", str(DIST), "--quiet"],
            cwd=ROOT,
            check=True,
        )
    versions = locked_versions()
    pins = [f"{name}=={versions[name]}" for name in THIRD_PARTY]
    (DIST / "modules.txt").write_text("\n".join(pins) + "\n", encoding="utf-8")
    return pins


if __name__ == "__main__":
    pins = build()
    for p in sorted(DIST.iterdir()):
        print(p.name)
    print("pinned:", ", ".join(pins))
