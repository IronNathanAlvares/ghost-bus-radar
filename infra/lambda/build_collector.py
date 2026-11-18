"""Build the Lambda packages for Python 3.11 on arm64: the collector (poller + static watcher)
and the API (ADR-0012).

    uv run python infra/lambda/build_collector.py      # -> infra/lambda/dist/{collector,api}.zip
    uv run python infra/lambda/build_collector.py api  # just one

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
DIST = Path(__file__).resolve().parent / "dist"
#: package name -> (the workspace package whose locked dependencies it needs, what to install)
PACKAGES = {
    "collector": ("ghostbus-ingest", ["packages/core", "packages/ingest"]),
    "api": ("ghostbus-api", ["packages/core", "packages/ingest", "packages/api"]),
}
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


def requirements(package: str) -> list[str]:
    out = subprocess.run(
        ["uv", "export", "--package", package, "--no-dev", "--no-hashes", "--frozen"],
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


def build(name: str) -> Path:
    package, sources = PACKAGES[name]
    out_zip = DIST / f"{name}.zip"
    with tempfile.TemporaryDirectory() as tmp:
        target = Path(tmp) / "pkg"
        req_file = Path(tmp) / "requirements.txt"
        req_file.write_text("\n".join(requirements(package)) + "\n", encoding="utf-8")
        uv = ["uv", "pip", "install", "--quiet", "--target", str(target), *PLATFORM]
        subprocess.run([*uv, "--only-binary", ":all:", "-r", str(req_file)], check=True, cwd=ROOT)
        subprocess.run([*uv, "--no-deps", *sources], check=True, cwd=ROOT)
        junk = [*target.rglob("__pycache__"), *target.glob("*.dist-info"), target / "bin"]
        for d in junk:
            shutil.rmtree(d, ignore_errors=True)
        (target / ".lock").unlink(missing_ok=True)  # uv's install lock

        out_zip.parent.mkdir(parents=True, exist_ok=True)
        files = sorted(p for p in target.rglob("*") if p.is_file())
        with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as z:
            for f in files:
                info = zipfile.ZipInfo(f.relative_to(target).as_posix(), FIXED_TIME)
                info.external_attr = 0o644 << 16
                info.compress_type = zipfile.ZIP_DEFLATED
                z.writestr(info, f.read_bytes())
    return out_zip


if __name__ == "__main__":
    for name in sys.argv[1:] or list(PACKAGES):
        path = build(name)
        print(f"{path} {path.stat().st_size / 1e6:.1f} MB")
    sys.exit(0)
