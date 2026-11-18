"""Copy the NTA API key from .env into SSM Parameter Store as a SecureString.

    uv run python infra/scripts/put_nta_key.py            # uses AWS_PROFILE / AWS_REGION from .env

The key is read from .env and sent straight to SSM: it is never printed, never passed on a
command line (where shell history and process lists would see it), and never enters Terraform
state (Terraform only knows the parameter's name). Standard parameters are free.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
NAME = "/ghostbus/nta-api-key"


def read_env(path: Path) -> dict[str, str]:
    out = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def main() -> int:
    env = read_env(ROOT / ".env")
    key = env.get("NTA_API_KEY", "")
    if not key:
        print("NTA_API_KEY is empty in .env", file=sys.stderr)
        return 1
    profile = env.get("AWS_PROFILE") or os.environ.get("AWS_PROFILE") or None
    region = env.get("AWS_REGION") or "eu-north-1"
    ssm = boto3.Session(profile_name=profile, region_name=region).client("ssm")
    ssm.put_parameter(
        Name=NAME,
        Description="NTA GTFS-R API key (ghost-bus-radar poller)",
        Value=key,
        Type="SecureString",
        Overwrite=True,
        Tier="Standard",
    )
    meta = ssm.describe_parameters(ParameterFilters=[{"Key": "Name", "Values": [NAME]}])[
        "Parameters"
    ][0]
    print(f"stored {NAME} (version {meta['Version']}, {len(key)} characters) in {region}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
