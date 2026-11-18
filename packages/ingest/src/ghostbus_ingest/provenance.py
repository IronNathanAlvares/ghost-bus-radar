"""Seal archive days into the hash chain, and verify the chain (``ghostbus_core.provenance``).

    python -m ghostbus_ingest.provenance seal                       # every complete, unsealed day
    python -m ghostbus_ingest.provenance verify --from 2026-11-15   # recompute and compare

``ARCHIVE_URI`` selects the archive (S3 in the cloud). A seal is written once with a conditional
create and is never overwritten; the bucket policy also forbids deleting it.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from ghostbus_core.provenance import (
    GENESIS,
    ObjectEntry,
    Seal,
    day_prefixes,
    make_seal,
    seal_key,
)
from ghostbus_ingest.archive import Archive, archive_from_env

#: The first day the archive holds data (collection started 2026-11-15 19:31 UTC).
FIRST_DAY = date(2026, 11, 15)


def entries(archive: Archive, day: date) -> list[ObjectEntry]:
    return [
        ObjectEntry(key, size, etag)
        for prefix in day_prefixes(day)
        for key, size, etag in archive.list_objects(prefix)
    ]


def read_seal(archive: Archive, day: date) -> Seal | None:
    raw = archive.get_bytes(seal_key(day))
    return Seal.from_json(json.loads(raw)) if raw else None


def seal_through(archive: Archive, last: date, first: date = FIRST_DAY) -> list[Seal]:
    """Seal every day from ``first`` to ``last`` that has no seal yet, in order. Returns new seals.

    Days must be complete: the caller passes a ``last`` that has ended (yesterday or earlier).
    """
    new: list[Seal] = []
    prev = GENESIS
    day = first
    while day <= last:
        existing = read_seal(archive, day)
        if existing is None:
            seal = make_seal(day, entries(archive, day), prev)
            body = json.dumps(seal.to_json(), indent=2).encode()
            if not archive.create_if_absent(seal_key(day), body):
                raise RuntimeError(f"seal for {day} appeared while sealing; not overwriting it")
            new.append(seal)
            existing = seal
        elif existing.prev_chain != prev:
            raise RuntimeError(f"seal for {day} does not link to the seal before it")
        prev = existing.chain
        day += timedelta(days=1)
    return new


@dataclass(frozen=True)
class Verdict:
    day: date
    ok: bool
    detail: str


def verify(archive: Archive, first: date, last: date) -> list[Verdict]:
    """Recompute each sealed day from the archive and compare with its stored seal and links."""
    out = []
    prev = GENESIS
    day = FIRST_DAY
    while day <= last:
        stored = read_seal(archive, day)
        if stored is None:
            if day >= first:
                out.append(Verdict(day, False, "not sealed"))
            break
        again = make_seal(day, entries(archive, day), prev)
        if day >= first:
            if stored.prev_chain != prev:
                out.append(Verdict(day, False, "seal does not link to the day before"))
            elif again.chain != stored.chain:
                out.append(
                    Verdict(
                        day,
                        False,
                        f"archive changed since sealed: {stored.objects} objects / "
                        f"{stored.bytes:,} B sealed, {again.objects} / {again.bytes:,} B now",
                    )
                )
            else:
                out.append(Verdict(day, True, f"{again.objects} objects, chain {again.chain[:16]}"))
        prev = stored.chain
        day += timedelta(days=1)
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("seal")
    v = sub.add_parser("verify")
    v.add_argument("--from", dest="first", type=date.fromisoformat, default=FIRST_DAY)
    v.add_argument("--to", dest="last", type=date.fromisoformat, default=None)
    args = parser.parse_args(argv)
    archive = archive_from_env(os.environ)
    yesterday = datetime.now(UTC).date() - timedelta(days=1)
    if args.cmd == "seal":
        for s in seal_through(archive, yesterday):
            print(f"sealed {s.day}: {s.objects} objects, {s.bytes:,} B, chain {s.chain}")
        return 0
    verdicts = verify(archive, args.first, args.last or yesterday)
    for x in verdicts:
        print(f"{x.day}  {'OK  ' if x.ok else 'FAIL'}  {x.detail}")
    return 0 if verdicts and all(x.ok for x in verdicts) else 1


if __name__ == "__main__":
    sys.exit(main())
