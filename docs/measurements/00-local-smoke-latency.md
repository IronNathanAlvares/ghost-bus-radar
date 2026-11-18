# Measurement 00: local Kafka to Iceberg latency

**Date:** 15 November 2026. **Sprint:** S0. **Command:**
`docker compose -f local/docker-compose.yml run --rm spark python -m ghostbus_jobs.smoke --events 50 --interval 1 --trigger-ms <t>`

## What is measured

Time from the producer creating an event to the commit of the Iceberg snapshot that made it
queryable. The commit time comes from the table's own metadata (`entries` joined to
`snapshots` on the data file each row lives in), not from polling, so it is exact to the
millisecond. Both clocks are the same container clock.

Path: confluent-kafka idempotent producer -> Kafka 4.1.1 (KRaft) -> Spark 3.5.6 Structured
Streaming Kafka source -> Iceberg 1.10 streaming sink -> REST catalog -> SeaweedFS.

## Results (50 events, one per second, laptop: 16 vCPU, 8 GB to Docker)

| Trigger | Landed | p50 | p95 | max |
|---|---|---|---|---|
| 500 ms | 50 / 50 | 740 ms | 1,220 ms | 3,379 ms |
| 1,000 ms | 50 / 50 | 987 ms | 2,185 ms | 3,687 ms |

A first 20-event run at 500 ms gave p50 784 ms, p95 1,601 ms.

## Reading it

- Every micro-batch takes 750 to 800 ms regardless of size, almost all of it the Iceberg commit
  (write a Parquet file, write manifests, swap the pointer through the REST catalog). At a 500 ms
  trigger Spark logs "batch is falling behind" on every batch, so the effective cadence is the
  commit time, not the trigger.
- The max (about 3.5 s) is always the first batch: JVM warm-up and table loading.
- **Implication for the real pipeline:** the brief's latency need is minutes (a bus is late or it
  is not), so the cloud jobs should use a 30 to 60 s trigger. That means about 1,440 to 2,880
  commits a day per table instead of about 100,000, far fewer small files for S3 Tables to compact,
  and lower request costs. Sub-second Iceberg commits are possible but buy nothing here.
- Nothing here exercises failure. Exactly-once under restarts is Sprint 3's chaos test.
