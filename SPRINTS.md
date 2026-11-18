# Ghost Bus Radar, sprint plan

**The working document.** Tick things off as they land. If a sprint slips, move the task; do not
silently drop it. The why behind each item is in `PLAN.md`.

**Rule for every sprint:** it ends with something that runs and a number that is measured.

**Status key:** `todo` / `doing` / `done` / `blocked` / `dropped`

---

## S0, foundations

| Task | Status |
|---|---|
| `uv` workspace: `packages/core`, `ingest`, `jobs`, `api`; ruff, mypy, pytest | done |
| `local/docker-compose.yml`: Kafka 4.1 KRaft (single node), Spark 3.5, SeaweedFS, Iceberg REST catalog | done |
| Smoke test: produce one message, Spark reads it, writes an Iceberg row, query it back | done |
| GitHub Actions: lint, typecheck, unit tests, compose integration test | done (not yet run on GitHub: no remote) |
| ADR-0001 raw archive before Kafka; ADR-0002 natural keys; ADR-0003 MSK provisioned not Serverless; ADR-0004 local mirrors cloud | done |

**Measure:** local end-to-end latency for one event.
**Result:** 50/50 events, produce-to-queryable p50 740 ms, p95 1.2 s at a 500 ms trigger. Iceberg commit (~780 ms) is the floor. See `docs/measurements/00-local-smoke-latency.md`.

## S1, start collecting (the clock starts here, so this is the priority)

| Task | Status |
|---|---|
| NTA API key in `.env`, verified 15 Nov 2026 (TripUpdates 200, 2,036 entities) | done |
| Read the NTA developer usage policy, record what it allows (`docs/02-nta-usage-policy.md`) | done |
| Schedule within the policy: one call per minute for the whole API, feeds alternate (ADR-0005) | done |
| Poller: fetch TripUpdates, Vehicles, Alerts; no immediate retries, backoff on failure; protobuf (zstd) to `raw/feed=…/dt=…/hh=…/` | done |
| Poller: normalise entities to JSON (schemas in `ghostbus_core/schemas/`), publish to Kafka with the idempotent producer, DLQ for bad entities, heartbeat per call (archive and Kafka) | done |
| Restart-safe spacing: a new poller reads its last call from the archive | done |
| Static watcher: hourly HEAD/ETag check of `GTFS_Realtime.zip`, archive each new version by SHA-256 with a manifest | done |
| Local collector running in Docker (`--profile collect`), archive in `~/ghostbus-archive` | done, collecting since 15 Nov 2026 19:31 UTC |
| `docs/measurements/01-feed-profile.md`: sizes, entities, `schedule_relationship` mix, vehicle coverage per operator, header lag | done (first 17 min); re-run after 72 h |
| ~~Measure the real rate limit by backing off on 429~~ | dropped: the policy publishes the limit, and probing above it would break the policy. 429s are recorded instead |
| Fetch success over 72 hours (`feed_profile` uptime table) | done 18 Nov: laptop about 33% (asleep), cloud 123 of 123 minutes since the cutover; 0 HTTP 429 (measurement 01, update) |
| Billing alarms ($10, $50, $150) via Terraform, then deploy poller + archive bucket to AWS | blocked: AWS account (you asked to do this later); moved to S5 |

**Measure:** fetch success rate over 72 hours; feed profile numbers.
**Result so far:** 17/17 scheduled calls collected, 0 throttled; 2,152 trip updates and 792
vehicles per snapshot; 38 live cancellations per snapshot; archive grows ~111 MB/day.
Findings that change later sprints: route must come from the static trip (realtime rail
`route_id`s differ), stale vehicle positions persist up to ~28 min, no vehicles for rail/Luas.

## S2, the timetable as history

| Task | Status |
|---|---|
| Profile the static feed for key problems: Irish Rail routes all named `rail`, ~30 duplicate Luas trip pairs a weekday, DST day inside the feed | done |
| Parse static GTFS; derive natural keys for route, stop, trip instance (rail adds long name; duplicate index) (ADR-0006) | done |
| SCD2 `dim.route`, `dim.stop` by one `MERGE` each; `dim.feed_version` with valid_from / valid_to as the commit marker | done |
| Per-version `route_version`, `trip`, `stop_time`, `service_date`; partition overwrite, idempotent reruns, self-retry | done |
| Trip-instance expansion per service day: `calendar` + `calendar_dates`, times over 24:00, DST (GTFS noon-minus-12h origin) | done |
| Property-based tests (Hypothesis): calendar vs brute-force oracle, wall-clock times every day 2024-2032, SCD2 invariants over random histories | done |
| Spark SCD2 result equals the pure reference implementation (integration test) | done |
| Two versions, NTA-shaped (all ids regenerated, renamed/withdrawn/new route, renamed stop): old trips resolve to old definitions | done (integration test) |
| Same proof on two **real** archived versions | done 18 Nov: 4 real versions; unmatched 0.96% / 1.38%, all in a 75-minute window after the 15 Nov republish (realtime ids lag the publish); fix is roadmap 1.0 (measurement 02, transition) |
| `rt_match_report`: realtime against the live version (`docs/measurements/02-timetable-match.md`) | done |

**Measure:** share of realtime trip ids that match the live static version (unmatched rate).
**Result:** 0.00% unmatched over 49,344 realtime trips (TripUpdates and Vehicles), 100% resolved
to a trip instance, realtime `start_time` equals the scheduled first departure in 100%. One
static version so far; the number around a version change is still to measure.

## S3, streaming bronze and silver, correctness first

| Task | Status |
|---|---|
| `bronze` Kafka to Iceberg with checkpointing; Kafka lineage on every row (ADR-0007) | done |
| `silver`: event time, 10 min watermark, `dropDuplicatesWithinWatermark` on natural keys (trip updates, vehicle positions, heartbeats), stop-time explode | done |
| Spark payload schemas checked against the normaliser output (no field lost) | done |
| Late-event test (5 min kept, 30 min dropped and counted, still in bronze; watermark survives restart) | done |
| Chaos test: SIGKILL the whole pipeline 20 times, compare row for row with a clean run (CI: 10 kills) | done |
| Metrics listener: batch duration, input/output rows, late rows, state size, watermark lag (JSONL locally) | done |
| Persistent local catalog (SQLite on a volume) and checkpoint volume; `ResolvingFileIO` so Iceberg stream offsets can live in `file://` checkpoints | done |
| `streamer` compose service and `stream_report` (live latency and metrics) | done |

**Measure:** duplicates after chaos run (target 0); event-to-silver latency p50 / p95.
**Result:** 0 duplicates, 0 missing, 0 extra rows in all five tables after 20 SIGKILLs, 16 of
them mid-batch (9 orphan files show kills between data write and commit). Live fetch-to-silver
latency p50 46 s, p95 47 s at a 30 s trigger; every micro-batch under 1 s. See
`docs/measurements/03-streaming.md`.

## S4, ghosts, reconcile, backfill

| Task | Status |
|---|---|
| `docs/03-ghost-definition.md`: outcomes, thresholds, honesty rules | done |
| Pure classifier `ghostbus_core.outcomes` with property tests (a collection gap can never create a ghost; closing a gap only makes outcomes more specific; raising the RAN threshold never creates runs) | done |
| Progress from geometry (NTA positions carry no stop sequence): furthest stop within 250 m | done |
| `NO_DATA` from heartbeat gaps (`ghostbus_core.coverage`) | done |
| `trip_state` `foreachBatch` on silver heartbeats: provisional outcomes, data clock, `MERGE` (compose service `trip-state`) | done |
| `reconcile` from the raw archive (executors read it directly); backfill = reconcile over a date range; final never overwritten by provisional (ADR-0008) | done |
| Outage demo (`backfill_demo`): stream with a hole vs archive backfill vs uninterrupted | done (simulated hole in real data; see measurement 04) |
| Integration test: every outcome on a synthetic week spanning a timetable version change | done |
| Threshold sensitivity table (`outcome_report`) | done |
| Rates over fully collected trips only (`in_rates`): a bus tracked to the end survives a gap, a missing one does not | done (found in the data, measurement 04 §2.2) |
| Evidence bounded to each trip's close time (rules 4), so it is the same whenever and from wherever it is computed | done (found by the full-row backfill diff, §2.1) |
| Integration tests isolated from the live topics (`topic_prefix`); stream readers skip correction snapshots; `lake_audit` (every lake fetch must be in the archive) | done (§2.1) |
| Field check kit: instructions, CSV template, `field_check` comparison job | done |
| Field check: ~50 observed departures at 2 to 3 stops | **yours**: needs someone at a bus stop (`docs/04-field-check.md`) |

**Measure:** ghost / untracked rate per route with CIs; backfill diff (target 0 rows); field-check agreement.
**Result:**

- **Backfill diff: 0 of 10,528 rows** differ from the uninterrupted stream, on outcome, reason
  and every evidence column.
- A simulated one-hour outage turned 436 trips into `NO_DATA` and **0 into ghosts**.
- First rates come from one Sunday evening: 813 fully collected bus trips. Ran 87.2% (84.7-89.3),
  untracked 8.6% (6.9-10.7), vanished 2.6%; Bus Éireann untracked 16.4% against Dublin Bus 4.5%.
  The untracked rate does not move as the RAN threshold goes from 40% to 80%.
- No route has 30 trips yet, so per-route rates wait for a weekday.
- The field check is still to do.
- Four bugs were found by the measurements themselves and fixed: survivorship in rates, evidence
  depending on "now", test data in the live tables, and streams that would stop on a correction.
- See `docs/measurements/04-outcomes.md`.

## S5, AWS (cheap by design: ADR-0009)

| Task | Status |
|---|---|
| Owner: AWS account (new experience, Free plan, `eu-north-1`), root MFA, IAM user `nathan-admin` with a CLI access key, profile `ghostbus` (`docs/05-aws-setup.md` §1) | done |
| AWS CLI v2 and Terraform 1.17 installed | done |
| ADR-0009: always-on collection + nightly batch, streaming only in bursts; Lambdas outside any VPC (no NAT) | done |
| Stateless Lambda poller: plans from archived heartbeats like the local poller; one call per minute guaranteed by an S3 conditional-write claim; 8 tests incl. duplicates, late starts, backoff, laptop handover | done |
| Reproducible arm64 Lambda package (6.3 MB, no boto3/Kafka), smoke-tested in AWS's Lambda arm64 image | done |
| Terraform `bootstrap` (state bucket, $20 budget alerts) and `core` (archive, poller + static watcher, schedules off by default, two alarms); fmt + validate locally and in CI | done |
| NTA key into SSM without printing it or touching Terraform state (`infra/scripts/put_nta_key.py`) | done |
| Account rules mapped with the IAM policy simulator: Free-plan SCPs allow the collector, S3 Tables, Athena, Glue; deny EMR, MSK, Kinesis and every region but `eu-north-1` | done |
| Apply bootstrap (state bucket, $20 budget: earned $20 of credits) and core (22 resources, schedules off); NTA key in SSM | done, 18 Nov |
| Cloud static watcher tested: archived the 17 Nov timetable (141 MB) in 46 s | done |
| Cutover from the laptop poller (`docs/05-aws-setup.md` §4): archive pre-copied (443 MB, 4 min), laptop stopped 07:29:13, last delta synced and schedules on by 07:29:35 on 18 Nov | done |
| First cloud minutes: every call collected (1,221 to 3,274 entities); the first run after enabling was skipped because the scheduler fired 14.8 s late. Poll second moved to :25, which exposed a pacing bug: calls stayed locked on the second they first ran at. Fixed (`next_call_at` realigns once, tolerance 5 s, 3 new tests) and verified live: 07:42:15 then 07:43:25, then every minute at :25 | done |
| Lake layer (ADR-0010): S3 table bucket, federated `s3tablescatalog` (IAM mode), work bucket, Glue role, Athena workgroup with 1 GB scan cap | done |
| Nightly Glue 5.1 Flex chain (timetable load, then reconcile), failure emails; backfill 15 to 17 Nov verified in Athena; about $0.04 a night | done |
| Bugs found by the cloud run, fixed with failing-first tests: executors can't read driver `/tmp`; Glue args starting with `--` dropped; same-label timetable zips overwrote each other (15 Nov 7,003 -> 12,185 trips); S3 Tables refuses DROP (rebuild now empties) | done |
| Diagrams for presenting and interviews (`docs/diagrams/`, 27 Mermaid diagrams, each with walkthrough and interview Q&A, all checked to render) | done |
| `stream` layer for a burst: VPC, MSK (2 x t3.small, IAM auth), streaming job, archive replay through MSK; measure latency, then destroy | todo |
| Cost per day measured from Cost Explorer in each mode | per-run Glue costs measured (`docs/measurements/05-cloud.md`); Cost Explorer after a few days of data |
| Runbooks: cutover, park, destroy, burst, backfill | cutover and park written |

**Measure:** real cost per day in each mode; cloud end-to-end latency (burst).

## S6, API and website

| Task | Status |
|---|---|
| DynamoDB live trip state written from `trip_state` | replaced: the API reads the newest archived snapshot instead, no stream or table needed (ADR-0012) |
| API: departures by stop plate code, health (`ghostbus-api`, HTTP API; route reliability is in the public export) | done 18 Nov (ADR-0012) |
| "Will it come?" probability: history by route x day type x trip-start hour, shrunk toward the route (20-trip prior), conditioned on live status (`heuristic-v1`) | done 18 Nov; probabilities appear once a day passes the quality gates |
| Calibration on held-out days: Brier score + reliability diagram | todo |
| Website: league table, route page, stop lookup, heatmaps, methodology, NTA attribution | done 18 Nov (ADR-0013); Lighthouse 99-100 on every page and category |
| Daily JSON export so the site works when the cluster is parked | done 18 Nov (ADR-0011) |

**Measure:** API p95 latency: gateway p95 323 ms, target 500 ms (`docs/measurements/06-api.md`); Brier score versus a "always says it comes" baseline (roadmap 2.4).

## S7, dashboard and story

| Task | Status |
|---|---|
| Athena views for BI | todo |
| Power BI report (or Tableau) + screenshots in repo | todo |
| CloudWatch alarms: stale feed, unmatched-ID spike, job down | todo |
| README with headline numbers, architecture, how to run, limits | todo |
| 3-minute demo video; write-up post | todo |
| Before/after view for the Trapeze AVL pilot, if it has started on any route | todo |
