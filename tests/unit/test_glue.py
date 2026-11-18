"""The Glue glue code: argument parsing and staging timetables where executors can read them."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any

import pytest

from ghostbus_jobs.timetable_load import stage_for_spark

ENTRY = Path(__file__).resolve().parents[2] / "infra" / "glue" / "entry.py"


def _entry() -> Any:
    spec = importlib.util.spec_from_file_location("glue_entry", ENTRY)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_glue_args_reads_values_that_start_with_dashes() -> None:
    """Found on the first cloud backfill: ``--ARGS "--from ..."`` was silently dropped."""
    argv = [
        "--JOB_ID", "j_1", "--TASK", "reconcile", "--ARGS", "--from 2026-11-15 --to 2026-11-17",
        "--job-bookmark-option", "job-bookmark-disable",
    ]  # fmt: skip
    args = _entry().glue_args(argv, ("TASK", "ARGS", "LAKE_MODE"))
    assert args == {"TASK": "reconcile", "ARGS": "--from 2026-11-15 --to 2026-11-17"}


def test_unknown_task_fails_loudly() -> None:
    with pytest.raises(SystemExit):
        _entry().main(["--TASK", "nope"])


def test_stage_is_the_local_folder_without_a_stage_uri(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("STATIC_STAGE_URI", raising=False)
    assert stage_for_spark(tmp_path, "abc") == str(tmp_path)


def test_stage_copies_the_csvs_to_shared_storage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    src, shared = tmp_path / "x", tmp_path / "shared"
    src.mkdir()
    (src / "trips.txt").write_text("trip_id\n1\n", encoding="utf-8")
    (src / ".complete").write_text("ok", encoding="utf-8")
    monkeypatch.setenv("STATIC_STAGE_URI", shared.as_uri())
    where = stage_for_spark(src, "abc")
    assert where == f"{shared.as_uri()}/abc"
    assert (shared / "abc" / "trips.txt").read_text(encoding="utf-8") == "trip_id\n1\n"
    assert not (shared / "abc" / ".complete").exists()
