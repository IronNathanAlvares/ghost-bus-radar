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


def test_every_glue_task_imports_without_a_running_spark() -> None:
    """Glue imports the task module before any SparkSession exists, so a module-level Column
    (F.col at import time) crashes there while passing every test that starts Spark first.
    That is how the first cloud run of the quality job failed. Import each one in a fresh
    interpreter with no Spark."""
    import subprocess
    import sys

    for module in _entry().TASKS.values():
        done = subprocess.run(
            [sys.executable, "-c", f"import {module}"], capture_output=True, text=True, timeout=120
        )
        assert done.returncode == 0, f"{module}: {done.stderr[-600:]}"


def test_batch_cost_counts_only_the_nightly_chain() -> None:
    """The first email reported $0.322: it counted a day of manual rebuilds as "last night"."""
    from datetime import UTC, datetime, timedelta

    from ghostbus_jobs.quality import scheduled_dpu_seconds

    since = datetime(2026, 11, 19, 3, 0, tzinfo=UTC)
    at = since + timedelta(hours=1)
    runs = [
        {"TriggerName": "ghostbus-nightly", "StartedOn": at, "DPUSeconds": 110.0},
        {"TriggerName": "ghostbus-after-timetable", "StartedOn": at, "DPUSeconds": 430.0},
        {"StartedOn": at, "DPUSeconds": 555.0},  # a manual rebuild: no trigger
        {
            "TriggerName": "ghostbus-nightly",
            "StartedOn": since - timedelta(days=1),
            "DPUSeconds": 99.0,
        },
        {"TriggerName": "ghostbus-after-reconcile", "StartedOn": at},  # still running: no DPU yet
    ]
    assert scheduled_dpu_seconds(runs, since) == 540.0
    assert scheduled_dpu_seconds([{"StartedOn": at, "DPUSeconds": 555.0}], since) is None
