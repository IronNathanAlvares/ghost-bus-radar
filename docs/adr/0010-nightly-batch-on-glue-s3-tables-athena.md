# ADR-0010: Nightly batch on AWS Glue, tables in S3 Tables, queries in Athena

**Status:** accepted, 18 November 2026. Context: ADR-0009 (cheap cloud), ADR-0008 (outcomes).

## Context

The cloud needs the batch half of the pipeline: load new timetable versions (`timetable_load`)
and decide final outcomes from the raw archive (`reconcile`), every night. PLAN chose EMR
Serverless, but the account's Free plan denies EMR and MSK (ADR-0009). It allows AWS Glue,
S3 Tables, Athena and Lake Formation.

## Decision

- **Glue 5.1 Spark jobs.** Glue 5.1 runs Spark 3.5.6, Python 3.11 and Iceberg 1.10.0, the same
  versions as the local stack, so the jobs run unchanged.
  - One entry script (`infra/glue/entry.py`) maps job parameters to the environment the jobs
    already read, and calls the same `main()` the laptop calls.
  - Our three packages ship as wheels. The Glue job installs them plus four pinned third-party
    modules (from `uv.lock`) with `--no-deps`, so pip can never install a second PySpark.
  - Two jobs, `ghostbus-timetable-load` and `ghostbus-reconcile`. A scheduled trigger starts the
    first at 04:15 UTC, after NTA's nightly static upload (about 23:30) and the close of
    yesterday's last trips (about 03:00). A conditional trigger runs the second when the first
    succeeds.
  - **Flex execution, 2 G.1X workers** (the minimum, $0.29 per DPU-hour). A Flex job may start
    late, which a nightly batch can afford.
  - A failed run emails the alerts topic via EventBridge.
- **S3 Tables as the Iceberg catalog**, through its Iceberg REST endpoint with SigV4. This is
  the same REST protocol as the local `iceberg-rest` fixture, so `ghostbus_core.lake` is the only
  place that knows the difference (ADR-0004). Table maintenance (compaction, snapshot expiry) is
  done by S3 Tables.
- **Athena through the federated `s3tablescatalog`, in IAM mode** (no Lake Formation grants), in a
  workgroup capped at 1 GB scanned per query. Every table and column name is lower case, as the
  integration requires.
- **Everything in Terraform:** table bucket, federated catalog, work bucket, wheels, jobs,
  triggers, failure rule, Athena workgroup. The triggers stay off (`batch_enabled`) until a
  manual run has worked.

## Consequences

- Cost is per job run, nothing when idle. Measured costs are in `docs/measurements/05-cloud.md`.
- If the account is later upgraded to the Paid plan, EMR Serverless becomes possible. The jobs
  would not change, only `infra/`.
- Glue starts each job with a fresh Python environment and installs the modules from PyPI, which
  adds about a minute per run.
- The streaming half (Kafka, silver, `trip_state`) stays local. In the cloud, outcomes are final
  and daily (ADR-0009).
