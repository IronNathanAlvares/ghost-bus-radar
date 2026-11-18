# Handoff

If you are picking this up cold, read in this order.

1. [`README.md`](README.md), five minutes, what it is and how to run it
2. [`PLAN.md`](PLAN.md), the design, especially section 6 (what counts as a ghost)
3. [`SPRINTS.md`](SPRINTS.md), what to do next

---

## State: Sprint 4 done, 15 November 2026

**Live data is being collected.** The `poller` and `static-watcher` containers (compose profile
`collect`, `restart: unless-stopped`) have archived the NTA feeds since 19:31 UTC on
15 November 2026, into `~/ghostbus-archive` on the host (outside OneDrive, not a Docker volume).
Collection only runs while the laptop and Docker Desktop are on; gaps show in the heartbeats and
become `NO_DATA`, never ghosts. Moving the poller to AWS is the first task of S5.

The timetable is loaded into `lake.dim` as history (ADR-0006) and realtime joins it with 0.00%
unmatched trip ids (`docs/measurements/02-timetable-match.md`). Open items carried forward:

- **18 November:** re-run `feed_profile` for the 72-hour fetch-success number (S1).
- **When NTA publishes a new static version** (`_state.json` in the archive changes): re-run
  `timetable_load` and `rt_match_report`, and record the unmatched rate around the switch (S2).

Sprint 3 added the streaming pipeline (`streamer` service, profile `stream`): Kafka to bronze to
silver, exactly once, proven by the chaos test (`docs/measurements/03-streaming.md`).

Sprint 4 added trip outcomes (`docs/03-ghost-definition.md`, ADR-0008):

- `trip-state` (profile `stream`) writes provisional outcomes to `lake.gold.trip_outcomes`.
- `reconcile` re-decides any date range from the archive as `final`. That is also the backfill.
- On 15 November, backfill equalled the stream on every column of 10,528 rows, and a simulated
  outage produced 0 ghosts (`docs/measurements/04-outcomes.md`).
- Sprint 5 (AWS) is under way, designed to be cheap (ADR-0009).
  - The account is a new-experience Free-plan account, pinned to `eu-north-1` with AWS-managed
    SCPs (`docs/05-aws-setup.md` §1). The CLI profile is `ghostbus` (IAM user `nathan-admin`).
  - Bootstrap and core are deployed, and **collection moved to AWS on 18 November at 07:29 UTC**.
    The cloud poller and static watcher write to `s3://ghostbus-archive-<account-id>/`.
  - **Never start the laptop `poller` or `static-watcher` again** while the schedule is on (one
    poller per key). The local streamer and Spark jobs work from `~/ghostbus-archive`; pull the
    cloud archive down first (`docs/05-aws-setup.md` §5).
  - Before the cutover the laptop collected only while awake: 471 and 393 of 1,440 calls on 16
    and 17 November.
  - **The lake is in the cloud too** (ADR-0010): S3 Tables `ghostbus-lake`, filled by a nightly
    Glue chain at 04:15 UTC (timetable load, then reconcile); query it in Athena, workgroup
    `ghostbus`, catalog `s3tablescatalog/ghostbus-lake`. Backfill any range with
    `aws glue start-job-run --job-name ghostbus-reconcile --arguments '{"--ARGS":"--from D --to D"}'`.
  - After reconcile, `ghostbus-quality` writes `gold.data_quality` and emails a daily summary
    (OK / WARN / FAIL). FAIL means: do not publish that day's numbers. The owner must confirm the
    SNS subscription email once, or nothing is delivered.
  - **The Free plan closes the account on 17 May 2027** unless upgraded. Keep the archive synced
    to the laptop.
  - The laptop lake was rebuilt with the new version keys on 18 November and matches the cloud on
    every day's trip instances (15 to 25 November). For local work, pull the archive first
    (`docs/05-aws-setup.md` section 5).
  - Diagrams for presenting the project: `docs/diagrams/README.md`.
  - **What's next:** `docs/06-roadmap.md`. Phase 0 is in progress: the code is on GitHub
    (`IronNathanAlvares/ghost-bus-radar`, public), and the archive guardrails and CloudTrail are live.
- The field check needs a person at a bus stop.
- `reconcile` is still run by hand: after each collected day, run it for that day, then
  `lake_audit`.

The local REST catalog now persists (SQLite on the `catalog` volume), as do streaming
checkpoints (`checkpoints` volume). `down -v` wipes lake, catalog and checkpoints together, which
keeps them consistent; reload with `timetable_load` and restart the streamer.

```bash
uv sync --all-packages
uv run ruff check . && uv run mypy && uv run pytest
docker compose -f local/docker-compose.yml up -d --wait
docker compose -f local/docker-compose.yml --profile collect up -d       # the collector
docker compose -f local/docker-compose.yml logs -f poller
docker compose -f local/docker-compose.yml run --rm spark pytest -m integration
docker compose -f local/docker-compose.yml run --rm spark python -m ghostbus_jobs.timetable_load
docker compose -f local/docker-compose.yml run --rm spark python -m ghostbus_jobs.rt_match_report
docker compose -f local/docker-compose.yml --profile stream up -d          # bronze + silver
docker compose -f local/docker-compose.yml run --rm spark pytest -m chaos -s  # ~10 min
docker compose -f local/docker-compose.yml run --rm spark python -m ghostbus_jobs.reconcile --from 2026-11-15 --to 2026-11-15
docker compose -f local/docker-compose.yml run --rm spark python -m ghostbus_jobs.outcome_report --from 2026-11-15 --to 2026-11-15
docker compose -f local/docker-compose.yml run --rm spark python -m ghostbus_jobs.backfill_demo --day 2026-11-15 --hole 20:45 21:45
docker compose -f local/docker-compose.yml run --rm spark python -m ghostbus_jobs.lake_audit
ARCHIVE_URI=file:///C:/Users/<you>/ghostbus-archive uv run python -m ghostbus_ingest.feed_profile
```

| Path | What |
|---|---|
| `packages/core/src/ghostbus_core/feeds.py` | Feed URLs and the one-call-per-minute schedule (ADR-0005) |
| `packages/core/src/ghostbus_core/pacing.py` | Next-call time, the 60 s spacing guarantee, backoff |
| `packages/core/src/ghostbus_core/gtfsr.py` | Protobuf to JSON records. The only code that knows the protobuf layout |
| `packages/core/src/ghostbus_core/schemas/` | JSON Schema for every Kafka payload |
| `packages/core/src/ghostbus_core/archive_layout.py` | Archive keys, identical locally and in S3 |
| `packages/core/src/ghostbus_core/topics.py` | Every Kafka topic, its partitions and retention |
| `packages/core/src/ghostbus_core/lake.py` | Iceberg catalog config: local REST fixture or S3 Tables |
| `packages/ingest/src/ghostbus_ingest/poller.py` | Fetch, archive, publish, heartbeat; the minute loop |
| `packages/ingest/src/ghostbus_ingest/static_watcher.py` | Archives each static GTFS version by SHA-256 |
| `packages/ingest/src/ghostbus_ingest/feed_profile.py` | Report on the archive: uptime, sizes, content |
| `packages/core/src/ghostbus_core/timetable.py` | Natural keys, calendars, GTFS time to UTC, version rules, reference SCD2 |
| `packages/ingest/src/ghostbus_ingest/static_archive.py` | Lists and extracts archived static versions |
| `packages/jobs/src/ghostbus_jobs/timetable_load.py` | Static versions into `lake.dim` (SCD2 MERGE, per-version tables, trip instances) |
| `packages/jobs/src/ghostbus_jobs/rt_match_report.py` | Realtime vs the live timetable version (the S2 measure) |
| `packages/jobs/src/ghostbus_jobs/stream.py` | Streaming bronze and silver: watermarks, dedupe, explode (ADR-0007) |
| `packages/jobs/src/ghostbus_jobs/stream_metrics.py` | Listener: batch time, rows, late rows, watermark lag |
| `packages/jobs/src/ghostbus_jobs/stream_report.py` | Fetch-to-queryable latency and metrics summary (the S3 measure) |
| `tests/chaos/test_exactly_once.py` | SIGKILL the pipeline 20 times, compare with a clean run |
| `packages/core/src/ghostbus_core/outcomes.py` | The outcome classifier (pure), `in_rates`, Wilson intervals, `RULES_VERSION` |
| `packages/core/src/ghostbus_core/coverage.py` | Collection gaps from heartbeats |
| `packages/jobs/src/ghostbus_jobs/outcomes.py` | Evidence builder over silver-shaped frames, from silver or from the archive; gold MERGE |
| `packages/jobs/src/ghostbus_jobs/trip_state.py` | Provisional outcomes as trips close (service `trip-state`) |
| `packages/jobs/src/ghostbus_jobs/reconcile.py` | Final outcomes from the archive; backfill = any date range |
| `packages/jobs/src/ghostbus_jobs/backfill_demo.py` | Stream vs stream-with-outage vs archive, row for row |
| `packages/jobs/src/ghostbus_jobs/outcome_report.py` | Rates with Wilson intervals, sensitivity (the S4 measure) |
| `packages/jobs/src/ghostbus_jobs/lake_audit.py` | Every lake `fetch_id` must be in the archive; `--delete` cleans every layer |
| `packages/jobs/src/ghostbus_jobs/field_check.py` | Field observations vs computed outcomes |
| `packages/jobs/src/ghostbus_jobs/smoke.py` | Kafka to Spark to Iceberg round trip with latency |
| `tests/gtfs_builder.py` | Two NTA-shaped static versions for timetable tests |
| `local/` | Compose stack, Spark image (archive mounted read-only), ingest image |
| `packages/ingest/src/ghostbus_ingest/lambda_handlers.py` | Cloud poller (stateless pacing, minute claim) and static watcher |
| `infra/lambda/build_collector.py` | Reproducible arm64 Lambda zip |
| `infra/terraform/bootstrap`, `infra/terraform/core` | State bucket and budget; archive, collector, alarms |
| `infra/scripts/put_nta_key.py` | NTA key from `.env` into SSM, never printed |
| `infra/glue/entry.py`, `infra/glue/build_jobs.py` | Glue entry point; wheels + pinned modules for Glue |
| `infra/terraform/core/lake.tf` | Table bucket, federated catalog, Glue jobs and triggers, Athena workgroup |
| `docs/diagrams/` | 27 diagrams with walkthroughs and interview Q&A |
| `docs/05-aws-setup.md` | Account setup, bootstrap, cutover, park |
| `docs/adr/` | Ten decisions so far |

## Rules for the collector

- **One poller per API key, anywhere.** Two would break the NTA limit. Before starting the AWS
  poller, stop the local one: `docker compose -f local/docker-compose.yml stop poller`.
- **Never call the live API in a loop from a test.** `poller --once <feed>` makes one manual
  call; use it only while the collector is stopped.
- The archive is the system of record. `down -v` cannot touch it (it wipes only the lake volume);
  deleting `~/ghostbus-archive` would lose history for good.

## Gotchas found in Sprint 1

- **Entity ids are positions** (`T1`, `T2`, ...), reassigned every snapshot. Dedupe on `trip_key`.
- **Realtime rail `route_id`s can disagree with `trips.txt`** for the same `trip_id`
  (`DUB-GALWAY-I` vs `DUB-GALWAY-O`). Take the route from the static trip.
- **Stale vehicle positions** stay in the Vehicles feed for up to ~28 minutes.
- **A restarted poller** used to be able to call within 60 s of the old process; it now reads its
  last call from the archived heartbeats.
- **Windows:** operator names have Irish characters (`feed_profile` forces UTF-8 output), and
  Python has no zoneinfo database (`tzdata` is a core dependency).

## Gotchas found in Sprint 0

- **MinIO images are gone** from Docker Hub; SeaweedFS replaces it (ADR-0004).
- **IPv6 localhost:** inside the SeaweedFS container `localhost` resolves to `::1` but the
  servers listen on IPv4. Health checks use `127.0.0.1`.
- **Docker Desktop on this machine** once failed to start because of a stale
  `%LOCALAPPDATA%\Docker\run\sailor-ingest.sock` that Windows cannot delete. Deleting it from WSL
  (`rm /mnt/c/Users/<you>/AppData/Local/Docker/run/sailor-ingest.sock`) fixed it.
- **Static GTFS is 586 MB unzipped.** `GTFS_Realtime/` is git-ignored and Docker-ignored.
- **The REST fixture catalog is in memory.** Restarting `iceberg-rest` forgets tables while their
  files stay in SeaweedFS. Use `down -v` for a clean slate.
- **The ruff cache** can hit "Access is denied" under OneDrive; harmless.

## Gotchas found in Sprint 2

- **Irish Rail routes are all named `rail`;** rail route keys include the long name.
- **About 30 Luas trip pairs a weekday are identical** apart from `block_id`; trip-instance keys
  carry a duplicate index.
- **GTFS times count from local noon minus 12 h,** not midnight: on 28 March 2027 the origin is
  23:00 the evening before. Never compute "midnight + seconds".
- **SeaweedFS occasionally truncates a Parquet read** under heavy parallel load (a local-only
  issue; real S3 does not do this). `timetable_load` retries itself; every step is idempotent.
- **A service day only has a schedule if a version was live at 04:00 that day,** so 14 November
  2026 (before our first archived version) has none.

## Gotchas found in Sprint 3

- **The Iceberg streaming source writes offsets through the table's FileIO,** so the catalog
  uses `ResolvingFileIO` (S3 for data, Hadoop for `file://` checkpoints).
- **Changing `pyproject.toml` rebuilds the Spark image's dependency layer** (several minutes).
- **Silver input rows count all bronze rows;** each silver query filters its own topic.
- **Fetch-to-bronze latency is set by phase:** poller second-of-minute versus trigger times.
- **The chaos test leaves orphan Parquet files** by design; they are unreferenced debris.

## Gotchas found in Sprint 4

- **Integration tests must never use the live topics.** The local stack is also the live
  pipeline. One test published a fixture to the live topics and it reached silver, including
  fake heartbeats that could hide real gaps. Tests use throwaway or prefixed topics
  (`KafkaPublisher(topic_prefix=...)`); `lake_audit` checks the lake against the archive.
- **Iceberg streaming readers stop on delete or overwrite snapshots** unless told to skip them
  (`stream.SKIP_CORRECTIONS`). A correction must therefore be applied to every layer, which
  `lake_audit --delete` does. A DELETE can conflict with a concurrent stream append; it is
  idempotent, so retry.
- **Rates only over fully collected trips** (`in_rates`): a bus tracked to the end survives a
  gap, a missing one does not, so counting every decided trip inflated "ran" from 87% to 94%.
- **Evidence stops at the trip's close time.** Late buses stay in the feeds; counting to "now"
  made the same trip differ between runs.
- **NTA sometimes marks a trip `CANCELED` in every snapshot while a bus runs it** (14 trips on
  the first evening). The outcome is `RAN`; the false cancellation is a finding in itself.
- **The leaked fixture messages stay in the local Kafka topics until about 22 November**
  (7-day retention). Do not restart the streamer with a fresh checkpoint before then without
  running `lake_audit --delete` afterwards.
- **EventBridge Scheduler fires about 6 s into the minute** (15 s on the first run after enabling).
  The poller calls at :25 and skips its minute rather than call late. `collector_enabled = true`
  lives in the git-ignored `core/terraform.tfvars`, so a plain `terraform apply` keeps collection on.
- **Nobody can delete from `raw/`** in the archive bucket, admins included (bucket policy); all
  buckets refuse plain HTTP; CloudTrail `ghostbus-trail` logs management events for a year.
  The CLI access key needs rotating every 90 days (`docs/05-aws-setup.md` section 7).
- **Security** (`docs/07-security.md`, diagrams in `docs/diagrams/08-security.md`):
  - The archive is hash-chained daily (`provenance/`, sealed by the quality job; verify with
    `python -m ghostbus_ingest.provenance verify`).
  - Three tripwires email on: a key read by anyone but the poller, CloudTrail changes, and bucket
    guardrail changes. They also fire on your own deploys, by design.
  - CI's security job runs gitleaks, pip-audit (one triaged advisory) and checkov (triaged skips
    in `.checkov.yaml`).
  - Owner sign-in is an AWS Builder ID (Google + authenticator-app MFA); the IAM root user has no
    password, so IAM's `AccountMFAEnabled = 0` is expected, not a gap.
- **Git Bash rewrites `/ghostbus/...` into a Windows path** in AWS CLI arguments (SSM names): prefix
  `MSYS_NO_PATHCONV=1`.
- **Never build a Spark `Column` at module level** (`F.col` at import time): Glue imports task
  modules before Spark starts. `tests/unit/test_glue.py` imports each task without Spark.
- **NTA's realtime ids can lag a timetable publish** (76 minutes on 15 November): matching falls
  back to the previous version for 3 hours after each publish (rules version 5).
- **NTA re-uploads the static GTFS zip every night** (about 23:10 to 23:30 UTC), with new bytes
  even when `feed_version` is unchanged. Archive identity is the SHA-256, so expect one archived
  version per day. **The label is not unique:** two uploads can share `feed_info.feed_version` and
  differ in content, so a version's key is the label plus the first 8 hex of the SHA-256
  (`timetable.version_key`). Lakes built before 18 November need `timetable_load --rebuild`.
- **A full `timetable_load` needs about 4 GB.** With the streamer and trip-state running, Docker's
  8 GB ran out and the JVM was killed ("Answer from Java side is empty"). Stop those two first:
  `docker compose -f local/docker-compose.yml stop streamer trip-state`, then start them again.
- **`trip-state` needs more than Spark's default 1 GB driver:** it ran out of memory at
  midnight, when two service days are open (`SPARK_DRIVER_MEMORY: 3g` in compose). It also
  restarts, harmlessly, when SeaweedFS truncates a read while heavy batch jobs share the stack.
