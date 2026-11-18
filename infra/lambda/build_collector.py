"""Build the collector Lambda package (poller + static watcher) for Python 3.11 on arm64.

    uv run python infra/lambda/build_collector.py          # -> infra/lambda/dist/collector.zip

Installs Linux arm64 wheels whatever machine it runs on, from the locked versions in uv.lock.
Leaves out what the Lambda runtime already has (boto3 and its dependencies) and what collect
mode does not use (confluent-kafka: the Lambda archives only, ADR-0009). The zip is
reproducible: same lock file, same bytes, so Terraform only redeploys on real changes.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent / "dist" / "collector.zip"
#: In the Lambda runtime already, or unused in collect mode.
EXCLUDE = {
    "boto3",
    "botocore",
    "s3transfer",
    "jmespath",
    "python-dateutil",
    "six",
    "urllib3",
    "confluent-kafka",
}
PLATFORM = ["--python-platform", "aarch64-manylinux2014", "--python-version", "3.11"]
FIXED_TIME = (2026, 1, 1, 0, 0, 0)


def requirements() -> list[str]:
    out = subprocess.run(
        ["uv", "export", "--package", "ghostbus-ingest", "--no-dev", "--no-hashes", "--frozen"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    reqs = []
    for line in out.splitlines():
        line = line.strip()
        if not line or line.startswith(("#", "-e")):
            continue
        name = line.split("==")[0].split(";")[0].strip().lower()
        if name not in EXCLUDE:
            reqs.append(line)
    return reqs


def build() -> Path:
    with tempfile.TemporaryDirectory() as tmp:
        target = Path(tmp) / "pkg"
        req_file = Path(tmp) / "requirements.txt"
        req_file.write_text("\n".join(requirements()) + "\n", encoding="utf-8")
        uv = ["uv", "pip", "install", "--quiet", "--target", str(target), *PLATFORM]
        subprocess.run([*uv, "--only-binary", ":all:", "-r", str(req_file)], check=True, cwd=ROOT)
        subprocess.run([*uv, "--no-deps", "packages/core", "packages/ingest"], check=True, cwd=ROOT)
        junk = [*target.rglob("__pycache__"), *target.glob("*.dist-info"), target / "bin"]
        for d in junk:
            shutil.rmtree(d, ignore_errors=True)
        (target / ".lock").unlink(missing_ok=True)  # uv's install lock

        OUT.parent.mkdir(parents=True, exist_ok=True)
        files = sorted(p for p in target.rglob("*") if p.is_file())
        with zipfile.ZipFile(OUT, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as z:
            for f in files:
                info = zipfile.ZipInfo(f.relative_to(target).as_posix(), FIXED_TIME)
                info.external_attr = 0o644 << 16
                info.compress_type = zipfile.ZIP_DEFLATED
                z.writestr(info, f.read_bytes())
    return OUT


if __name__ == "__main__":
    path = build()
    print(f"{path} {path.stat().st_size / 1e6:.1f} MB")
    sys.exit(0)
