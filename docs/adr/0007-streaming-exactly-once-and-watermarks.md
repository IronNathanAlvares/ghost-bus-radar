# ADR-0007: Streaming bronze and silver: exactly once, watermarks, dedupe keys

**Status:** accepted, 15 November 2026

## Context

The brief asks for watermarks for late events and exactly-once writes into Iceberg. The data has
its own quirks (measurement 01): NTA entity ids are positions, not ids; every trip update carries
the snapshot time, not an observation time; and stale vehicle positions are repeated, unchanged,
for up to ~28 minutes.

## Decision

**Bronze** is one Kafka-to-Iceberg streaming query over all feed topics. Rows keep the payload
as JSON text plus `topic`, `kafka_partition`, `kafka_offset`. Exactly-once comes from the two
halves Spark and Iceberg each guarantee: Kafka offsets are in the checkpoint's offset log before
a batch runs, and Iceberg's streaming sink writes the query id and epoch into each snapshot and
skips an epoch it has already committed. `(topic, partition, offset)` is therefore unique.

**Silver** queries read bronze as an Iceberg stream (offsets are snapshot ids), so they are
exactly-once the same way, and add:

| Table | Event time | Dedupe key (within the watermark) | Why that key |
|---|---|---|---|
| `trip_updates` | feed header time | `trip_key`, event time | One row per trip per snapshot. Removes replayed fetches only |
| `vehicle_positions` | the vehicle's own timestamp | vehicle, `trip_key`, event time | A stale repeat is the same observation; keep it once |
| `heartbeats` | fetch time | `fetch_id` | One per API call |
| `stop_time_predictions` | from `trip_updates` | none (stateless explode) | Inherits the upstream dedupe |

- **Watermark: 10 minutes.** A row more than 10 minutes behind the newest event time seen is
  dropped from silver and **counted** (`numRowsDroppedByWatermark`, exported by the metrics
  listener as `late_rows`). It is not lost: bronze and the raw archive keep it, and the nightly
  reconcile (Sprint 4) recomputes from those. Dedupe state for a key is kept exactly as long as
  a duplicate could still arrive on time, so state stays bounded.
- **Nothing dedupes on NTA entity ids.** They are reassigned every snapshot.
- **Payloads that fail to parse** stay in bronze; silver filters rows without a `fetch_id`.
- **Kafka data loss does not stop the stream** (`failOnDataLoss=false`; tests set it to true).
  Kafka is transport; the archive is the record, and reconcile closes gaps.
- **One FileIO for both schemes.** The catalog uses Iceberg's `ResolvingFileIO`: the Iceberg
  streaming source writes its offsets into the Spark checkpoint through the table's FileIO,
  and checkpoints are `file://` locally while table data is `s3://`.
- **Checkpoints and the catalog persist** in Docker volumes locally (the REST catalog now uses
  SQLite on a volume), and live in S3 in AWS. Both are wiped together with the lake by `down -v`.

## How it is proven

- `tests/integration/test_stream.py`: replayed fetches and stale vehicle repeats are removed;
  an event 5 minutes late is kept, one 30 minutes late is dropped, counted once, and still
  present in bronze; the watermark survives a restart; Spark schemas lose no field.
- `tests/chaos/test_exactly_once.py`: the whole pipeline is SIGKILLed 20 times mid-flight and
  compared row for row with an uninterrupted run (`docs/measurements/03-streaming.md`).

## Consequences

- Silver lags bronze by one trigger (it reads bronze's commits), so fetch-to-silver latency is
  roughly two triggers plus two commits. At a 30 s trigger that is about a minute, far inside
  what ghost detection needs.
- A late row is visible in silver only after reconcile runs. Outcomes are final only after
  reconcile, which Sprint 4 makes explicit.
