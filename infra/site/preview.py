"""Preview the website locally, with the same security headers CloudFront sends.

    uv run python infra/site/preview.py            # synthetic demo data (never uploaded)
    uv run python infra/site/preview.py --live     # the real published data/v1 from S3

Serves ``web/`` on http://127.0.0.1:8770. ``/v1/*`` is forwarded to the deployed API (its URL
from ``terraform output api_url``), so the stop page shows real departures in both modes.

The demo data is made with the real export builders (``ghostbus_core.export.build``) from random
counts, so the pages can be checked with full tables and charts before any day has passed the
quality gates. Every demo file says "DEMO" in its attribution and the folder is temporary.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import subprocess
import tempfile
import urllib.error
import urllib.request
from datetime import UTC, date, datetime, timedelta
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from ghostbus_core.export import Cell, Stop, build, dumps

ROOT = Path(__file__).resolve().parents[2]
HEADERS = json.loads((Path(__file__).parent / "security_headers.json").read_text(encoding="utf-8"))
TERRAFORM_CORE = ROOT / "infra" / "terraform" / "core"

DUBLIN_BUS = ("1", "4", "7", "9", "13", "15", "16", "27", "39", "46A", "123", "145", "150")
ROUTES = (
    [("Dublin Bus", r) for r in DUBLIN_BUS]
    + [("Go-Ahead Ireland", r) for r in ("17", "45A", "59", "75", "175")]
    + [("Bus Éireann", r) for r in ("101", "X1", "105", "201", "220")]
)
DEMO_STOPS = [
    Stop("1358", "College Green", 53.34436, -6.25936, ("27", "150", "15", "39")),
    Stop("273", "Merrion Square", 53.33981, -6.24891, ("4", "7", "46A")),
    Stop("7602", "Dún Laoghaire Station", 53.29438, -6.13457, ("7", "46A", "75")),
]


def demo_docs(end: date) -> dict[str, dict]:  # type: ignore[type-arg]
    rnd = random.Random(42)
    days = [end - timedelta(days=i) for i in range(13, -1, -1)]
    quality = {d: "ok" for d in days}
    quality[days[2]] = "fail"
    quality[days[5]] = "warn"
    cells = []
    for agency, route in ROUTES:
        base = rnd.uniform(0.01, 0.14)
        for d in days:
            for hour in range(6, 24):
                n = rnd.randint(0, 9) if d.weekday() < 5 else rnd.randint(0, 4)
                if not n:
                    continue
                rush = 1.6 if hour in (8, 17, 18) else 1.0
                p = min(0.6, base * rush)
                ghost = sum(rnd.random() < p * 0.7 for _ in range(n))
                phantom = sum(rnd.random() < p * 0.3 for _ in range(n - ghost))
                vanished = sum(rnd.random() < 0.02 for _ in range(n - ghost - phantom))
                cancelled = sum(rnd.random() < 0.01 for _ in range(n - ghost - phantom - vanished))
                ran = n - ghost - phantom - vanished - cancelled
                cells.append(
                    Cell(d, agency, route, hour, n, ran, ghost, phantom, vanished, cancelled, 0)
                )
    now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    docs = build(cells, quality, DEMO_STOPS, end, now, "rules5-DEMO", "ab" * 32)
    for doc in docs.values():
        doc["attribution"] = dict(
            doc["attribution"], disclaimer="DEMO DATA, random numbers for a local preview."
        )
    return docs


def api_url() -> str | None:
    tf = shutil.which("terraform") or os.environ.get("TERRAFORM")
    if not tf:
        return os.environ.get("GHOSTBUS_API_URL")
    out = subprocess.run(
        [tf, "output", "-raw", "api_url"],
        cwd=TERRAFORM_CORE,
        capture_output=True,
        text=True,
        check=False,
    )
    return out.stdout.strip().rstrip("/") or os.environ.get("GHOSTBUS_API_URL")


class Handler(SimpleHTTPRequestHandler):
    api: str | None = None

    def end_headers(self) -> None:
        self.send_header("Content-Security-Policy", HEADERS["content_security_policy"])
        self.send_header("Permissions-Policy", HEADERS["permissions_policy"])
        self.send_header("Referrer-Policy", HEADERS["referrer_policy"])
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def do_GET(self) -> None:
        if self.path.startswith("/v1/") and self.api:
            try:
                with urllib.request.urlopen(self.api + self.path, timeout=15) as r:
                    body, status, ctype = r.read(), r.status, r.headers.get("content-type", "")
            except urllib.error.HTTPError as e:
                body, status, ctype = e.read(), e.code, "application/json"
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        super().do_GET()

    def log_message(self, fmt: str, *args: object) -> None:
        pass


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true", help="real data/v1 from S3 instead of demo data")
    ap.add_argument("--port", type=int, default=8770)
    args = ap.parse_args()
    tmp = Path(tempfile.mkdtemp(prefix="ghostbus-site-"))
    shutil.copytree(ROOT / "web", tmp, dirs_exist_ok=True)
    data = tmp / "data" / "v1"
    if args.live:
        import boto3

        profile = os.environ.get("AWS_PROFILE", "ghostbus")
        s = boto3.Session(profile_name=profile, region_name="eu-north-1")
        bucket = f"ghostbus-site-{s.client('sts').get_caller_identity()['Account']}"
        s3 = s.client("s3")
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="data/v1/"):
            for o in page.get("Contents", []):
                out = tmp / o["Key"]
                out.parent.mkdir(parents=True, exist_ok=True)
                s3.download_file(bucket, o["Key"], str(out))
    else:
        yesterday = datetime.now(UTC).date() - timedelta(days=1)
        for key, doc in demo_docs(yesterday).items():
            out = data / key
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_bytes(dumps(doc))
    Handler.api = api_url()
    print(
        f"serving {tmp} on http://127.0.0.1:{args.port} ({'live' if args.live else 'DEMO'} data; "
        f"API {'forwarded' if Handler.api else 'not available'})",
        flush=True,
    )
    server = ThreadingHTTPServer(("127.0.0.1", args.port), partial(Handler, directory=str(tmp)))
    server.serve_forever()


if __name__ == "__main__":
    main()
