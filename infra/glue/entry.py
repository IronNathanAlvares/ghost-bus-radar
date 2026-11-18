"""AWS Glue entry point for the batch jobs (ADR-0010).

Glue runs one script per job; this one dispatches to the same ``main()`` the laptop runs:

    --TASK timetable_load            load new static versions into lake.dim
    --TASK reconcile                 final outcomes for yesterday (or --ARGS "--from D --to D")
    --TASK outcome_report            rates for --ARGS "--from D --to D"
    --TASK quality                   checks + summary email for yesterday (or --ARGS "--date D")
    --TASK export                    public JSON for the website (or --ARGS "--to D")

Job parameters set the environment the jobs read (``LAKE_MODE``, ``LAKE_TABLE_BUCKET_ARN``,
``AWS_REGION``, ``ARCHIVE_URI``), so the code has no Glue-specific branches. Glue adds its own
arguments (``--JOB_ID``, ``--JOB_RUN_ID``, ...); only known keys are read.
"""

from __future__ import annotations

import importlib
import os
import shlex
import sys

TASKS = {
    "timetable_load": "ghostbus_jobs.timetable_load",
    "reconcile": "ghostbus_jobs.reconcile",
    "outcome_report": "ghostbus_jobs.outcome_report",
    "quality": "ghostbus_jobs.quality",
    "export": "ghostbus_jobs.export",
}
ENV_ARGS = (
    "LAKE_MODE",
    "LAKE_TABLE_BUCKET_ARN",
    "AWS_REGION",
    "ARCHIVE_URI",
    "STATIC_STAGE_URI",
    "ALERTS_TOPIC_ARN",
    "EXPORT_URI",
)


def glue_args(argv: list[str], keys: tuple[str, ...]) -> dict[str, str]:
    """The value after each ``--KEY`` we know, whatever it looks like.

    Values may themselves start with ``--`` (``--ARGS "--from 2026-11-15"``). A generic "skip
    anything that looks like a flag" parser dropped those silently on the first cloud backfill,
    so only known keys are read, each taking the next token as its value.
    """
    out = {}
    for key in keys:
        flag = f"--{key}"
        if flag in argv:
            i = argv.index(flag)
            if i + 1 < len(argv):
                out[key] = argv[i + 1]
    return out


def main(argv: list[str]) -> int:
    args = glue_args(argv, ("TASK", "ARGS", *ENV_ARGS))
    for key in ENV_ARGS:
        if key in args:
            os.environ[key] = args[key]
    task = args.get("TASK", "")
    if task not in TASKS:
        raise SystemExit(f"--TASK must be one of {sorted(TASKS)}, got {task!r}")
    module = importlib.import_module(TASKS[task])
    task_argv = shlex.split(args.get("ARGS", ""))
    print(f"ghostbus glue task={task} args={task_argv}", flush=True)
    code = int(module.main(task_argv) or 0)
    if code:
        raise SystemExit(code)  # Glue marks the run FAILED
    return 0


if __name__ == "__main__":
    main(sys.argv[1:])
