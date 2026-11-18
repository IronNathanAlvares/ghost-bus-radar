"""Tamper evidence for the raw archive: a hash chain over daily manifests. Pure.

Every published number is derived from the raw archive, so the archive must be provably
unchanged since the data was collected (``docs/07-security.md``). Each UTC day of the archive
(``raw/feed=*/dt=YYYY-MM-DD/``) is *sealed* once it is complete:

    manifest  = sorted lines "key<TAB>size<TAB>etag" of every object of that day
    chain[D]  = sha256( chain[D-1] + "\\n" + manifest )        chain[first day - 1] = GENESIS

Editing, adding or removing any object of any sealed day changes that day's hash and every hash
after it. The chain head is copied out of AWS daily (the summary email), so even someone able to
rewrite both the archive and its seals cannot hide it from an earlier copy.

Static timetable zips are left out: they are content-addressed already (their key is their
SHA-256), and ``_state.json`` is mutable by design.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date

GENESIS = "0" * 64
#: Feeds whose objects are partitioned by day and sealed.
SEALED_FEEDS = ("trip_updates", "vehicles", "alerts", "heartbeat")


@dataclass(frozen=True)
class ObjectEntry:
    key: str
    size: int
    etag: str


@dataclass(frozen=True)
class Seal:
    day: date
    objects: int
    bytes: int
    manifest_sha256: str
    prev_chain: str
    chain: str

    def to_json(self) -> dict[str, object]:
        return {
            "day": self.day.isoformat(),
            "objects": self.objects,
            "bytes": self.bytes,
            "manifest_sha256": self.manifest_sha256,
            "prev_chain": self.prev_chain,
            "chain": self.chain,
        }

    @classmethod
    def from_json(cls, d: dict[str, object]) -> Seal:
        return cls(
            day=date.fromisoformat(str(d["day"])),
            objects=int(str(d["objects"])),
            bytes=int(str(d["bytes"])),
            manifest_sha256=str(d["manifest_sha256"]),
            prev_chain=str(d["prev_chain"]),
            chain=str(d["chain"]),
        )


def day_prefixes(day: date) -> list[str]:
    return [f"raw/feed={feed}/dt={day.isoformat()}/" for feed in SEALED_FEEDS]


def seal_key(day: date) -> str:
    return f"provenance/dt={day.isoformat()}.json"


def manifest(entries: Iterable[ObjectEntry]) -> str:
    """Canonical text: one line per object, sorted by key, so listing order never matters."""
    rows = sorted(entries, key=lambda e: e.key)
    quote = '"'
    return "\n".join(f"{e.key}\t{e.size}\t{e.etag.strip(quote)}" for e in rows)


def make_seal(day: date, entries: Iterable[ObjectEntry], prev_chain: str) -> Seal:
    items = list(entries)
    text = manifest(items)
    m = hashlib.sha256(text.encode("utf-8")).hexdigest()
    chain = hashlib.sha256(f"{prev_chain}\n{text}".encode()).hexdigest()
    return Seal(day, len(items), sum(e.size for e in items), m, prev_chain, chain)
