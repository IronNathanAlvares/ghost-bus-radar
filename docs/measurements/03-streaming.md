# Measurement 03: streaming bronze and silver

**Date:** 15 November 2026. **Sprint:** S3. Design: ADR-0007.

## 1. Exactly-once under SIGKILL (chaos test)

```bash
docker compose -f local/docker-compose.yml run --rm spark pytest -m chaos -s
```

Input: 1,539 Kafka messages on fresh topics (27 TripUpdates and 27 Vehicles polls over 9
minutes, with every other vehicle position a stale repeat and 5 whole fetches replayed). The
pipeline (bronze plus four silver queries, 15 Kafka offsets and 1 Iceberg file per micro-batch,
so it is nearly always mid-batch) ran as its own process and was killed with **SIGKILL to the
whole process group, JVM included, 20 times**, 15 to 30 s after each start. It was then run to
completion and compared, row for row, with an uninterrupted run over the same input.

**Kills that landed mid-batch: 16 of 20.** After each kill the checkpoints were read: a batch
with an `offsets` entry and no `commits` entry was in flight. The first 16 kills each caught 2 to
5 queries mid-batch; the last 4 found the data fully processed.

| Table | Clean run rows | After 20 kills | Duplicates | Missing | Extra | Orphan files |
|---|---|---|---|---|---|---|
| bronze | 1,539 | 1,539 | 0 | 0 | 0 | 2 |
| silver trip_updates | 810 | 810 | 0 | 0 | 0 | 3 |
| silver vehicle_positions | 280 | 280 | 0 | 0 | 0 | 0 |
| silver heartbeats | 54 | 54 | 0 | 0 | 0 | 0 |
| silver stop_time_predictions | 1,982 | 1,982 | 0 | 0 | 0 | 4 |

- **Zero duplicates, zero missing, zero extra rows in every table.** Bronze also has exactly one
  row per `(topic, partition, offset)`.
- **Orphan files are the proof the kills hit writes.** Nine Parquet files were written by a task
  and never committed, because the process died between writing data and committing the Iceberg
  snapshot. None of them is visible to any reader. They cost storage only; Iceberg's
  `remove_orphan_files` maintenance (Sprint 5) cleans them.
- Dedupe worked identically under chaos: 540 vehicle position messages became 280 rows (stale
  repeats removed), and the 5 replayed fetches added nothing to silver.
- Run time: about 8 minutes. CI runs the same test with 10 kills.

## 2. Late events

`tests/integration/test_stream.py::test_late_events`: with the newest event at T+20 min (so a
watermark of T+10 min), an event at T+15 min (5 minutes behind) is written to silver; one at
T-10 min (30 minutes behind) is dropped from silver, counted exactly once as a late row by the
metrics listener, and is still in bronze for the nightly reconcile. The watermark carried over
correctly across a stop and restart of the job.

## 3. Live latency and metrics

The `streamer` service (30 s trigger, 10-minute watermark) on the live NTA feeds, started
20:47 UTC; measured over the 11 polls fetched from 20:52 to 21:02 (after it had caught up with
the backlog it replays from Kafka on first start).

```bash
docker compose -f local/docker-compose.yml --profile stream up -d
docker compose -f local/docker-compose.yml run --rm spark python -m ghostbus_jobs.stream_report --since 2026-11-15T20:52:00
```

**Fetch to queryable** (poller fetch time to the commit of the Iceberg snapshot holding the row,
from the tables' own `entries` and `snapshots` metadata):

| Table | Rows | p50 | p95 | max |
|---|---|---|---|---|
| bronze (all topics) | 12,668 | 16.4 s | 16.4 s | 16.4 s |
| silver trip_updates | 7,341 | 46.2 s | 46.5 s | 46.5 s |
| silver vehicle_positions | 2,913 | 46.2 s | 46.3 s | 46.3 s |
| silver heartbeats | 10 | 46.1 s | 46.2 s | 46.2 s |

**Why p50 equals p95:** the latency is set by the phase of two clocks, not by load. The poller
calls at second 14 of every minute; the 30 s trigger fires at :00 and :30. Each fetch therefore
waits 15.6 s for the :30 trigger, takes about 0.75 s to commit to bronze (16.4 s), and reaches
silver one trigger later (46 s). In general: bronze = 0 to 30 s of trigger wait plus about 1 s;
silver = one more trigger. Worst case about 62 s, against a requirement measured in minutes
(a bus is late, cancelled or missing). Aligning the trigger to the poller's second would cut
bronze to about 1 s if it ever mattered.

**Streaming metrics** (from the metrics listener, batches with input, same window):

| Query | Batches | Input rows | Batch s p50 / p95 | Late rows | Watermark lag s (p50) |
|---|---|---|---|---|---|
| bronze | 11 | 12,668 | 0.8 / 0.9 | 0 | - |
| silver_trip_updates | 11 | 12,561 | 0.6 / 0.9 | 0 | 768 |
| silver_vehicle_positions | 11 | 12,561 | 0.6 / 0.7 | **58** | 767 |
| silver_heartbeats | 11 | 12,561 | 0.6 / 0.6 | 0 | 706 |
| silver_stop_time_predictions | 5 | 9,153 | 0.4 / 0.5 | 0 | - |

- Every batch finishes in under a second: the pipeline uses about 3% of each 30 s trigger.
- **The 58 late vehicle rows are NTA's stale positions** (measurement 01: some positions are
  repeated for up to ~28 minutes). A position more than 10 minutes older than the newest one is
  dropped from silver and counted; it is still in bronze and the archive.
- Watermark lag (wall clock minus watermark) is the 10-minute delay plus the 2 to 3 minutes
  between polls of one feed, as expected.
- Input rows to the silver queries are bronze rows of all topics; each query filters its topic.

## Result against the Sprint 3 measure

| Measure | Target | Result |
|---|---|---|
| Duplicates after the chaos run | 0 | **0** in all five tables (also 0 missing, 0 extra) |
| Event-to-silver latency | p50 / p95 | **46 s / 47 s** live, at a 30 s trigger |
