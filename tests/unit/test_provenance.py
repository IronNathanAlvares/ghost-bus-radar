"""The archive hash chain: any edit, addition or deletion in any sealed day is detected, seals are
never overwritten, and the chain links every day to the one before."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from ghostbus_core.provenance import GENESIS, ObjectEntry, make_seal, manifest, seal_key
from ghostbus_ingest.archive import LocalArchive
from ghostbus_ingest.provenance import read_seal, seal_through, verify

D1, D2, D3 = date(2026, 11, 15), date(2026, 11, 16), date(2026, 11, 17)


def archive_with_days(tmp_path: Path) -> LocalArchive:
    a = LocalArchive(tmp_path)
    for day in (D1, D2, D3):
        for i in range(3):
            a.put_bytes(f"raw/feed=vehicles/dt={day}/hh=10/s{i}.pb.zst", f"{day}-{i}".encode(), "x")
        a.put_bytes(f"raw/feed=heartbeat/dt={day}/hh=10/h.json", b"{}", "x")
    a.put_bytes("raw/feed=static/_state.json", b"mutable", "x")  # never sealed
    return a


def test_seal_then_verify_is_clean(tmp_path: Path) -> None:
    a = archive_with_days(tmp_path)
    seals = seal_through(a, D3, first=D1)
    assert [s.day for s in seals] == [D1, D2, D3] and all(s.objects == 4 for s in seals)
    assert seals[0].prev_chain == GENESIS and seals[1].prev_chain == seals[0].chain
    assert all(v.ok for v in verify(a, D1, D3))
    assert seal_through(a, D3, first=D1) == []  # nothing left to seal


@pytest.mark.parametrize("change", ["edit", "add", "delete"])
def test_any_change_to_a_sealed_day_is_named(tmp_path: Path, change: str) -> None:
    a = archive_with_days(tmp_path)
    seal_through(a, D3, first=D1)
    key = f"raw/feed=vehicles/dt={D2}/hh=10/s1.pb.zst"
    if change == "edit":
        a.put_bytes(key, b"rewritten history", "x")
    elif change == "add":
        a.put_bytes(f"raw/feed=vehicles/dt={D2}/hh=11/forged.pb.zst", b"fake", "x")
    else:
        (tmp_path / key).unlink()
    verdicts = {v.day: v for v in verify(a, D1, D3)}
    assert verdicts[D1].ok and not verdicts[D2].ok
    assert "archive changed since sealed" in verdicts[D2].detail


def test_mutable_static_state_is_outside_the_chain(tmp_path: Path) -> None:
    a = archive_with_days(tmp_path)
    seal_through(a, D3, first=D1)
    a.put_bytes("raw/feed=static/_state.json", b"changed hourly by the watcher", "x")
    assert all(v.ok for v in verify(a, D1, D3))


def test_a_seal_is_never_overwritten(tmp_path: Path) -> None:
    a = archive_with_days(tmp_path)
    seal_through(a, D1, first=D1)
    before = a.get_bytes(seal_key(D1))
    a.put_bytes(f"raw/feed=vehicles/dt={D1}/hh=12/late.pb.zst", b"late", "x")
    seal_through(a, D2, first=D1)  # must not reseal D1 to hide the change
    assert a.get_bytes(seal_key(D1)) == before
    assert not {v.day: v for v in verify(a, D1, D2)}[D1].ok


def test_a_broken_link_is_reported(tmp_path: Path) -> None:
    a = archive_with_days(tmp_path)
    seal_through(a, D2, first=D1)
    forged = make_seal(D2, [], "f" * 64)  # a replaced seal that does not link to D1
    (tmp_path / seal_key(D2)).write_text(__import__("json").dumps(forged.to_json()))
    verdicts = {v.day: v for v in verify(a, D1, D2)}
    assert verdicts[D2].detail == "seal does not link to the day before"
    assert read_seal(a, D1) is not None


entry = st.builds(
    ObjectEntry,
    key=st.text("abcdef/=0123456789", min_size=1, max_size=20),
    size=st.integers(0, 10**6),
    etag=st.text("0123456789abcdef", min_size=32, max_size=32),
)


@settings(max_examples=200)
@given(st.lists(entry, min_size=1, max_size=12, unique_by=lambda e: e.key), st.randoms())
def test_manifest_ignores_listing_order_but_not_content(entries: list[ObjectEntry], rnd) -> None:  # type: ignore[no-untyped-def]
    shuffled = list(entries)
    rnd.shuffle(shuffled)
    assert manifest(shuffled) == manifest(entries)
    first = entries[0]
    tampered = [ObjectEntry(first.key, first.size + 1, first.etag), *entries[1:]]
    assert make_seal(D1, tampered, GENESIS).chain != make_seal(D1, entries, GENESIS).chain
