# ADR-0001: Archive every raw snapshot to S3 before publishing to Kafka

**Status:** accepted, 15 November 2026

## Context

The brief asks for a backfill path for outages. Kafka retention is days, Spark checkpoints only
remember offsets, and any part of the pipeline (poller, MSK, EMR) can be down. Separately, the
expensive parts of the stack (MSK, EMR Serverless) should not have to run all the time, but
history that was never collected can never be recovered.

## Decision

The poller writes each fetched GTFS-R snapshot, byte for byte as received (protobuf, zstd
compressed), to `s3://<archive>/raw/feed=<feed>/dt=<yyyy-mm-dd>/hh=<hh>/<fetch_ts>.pb.zst`
**before** it explodes entities into Kafka. The same applies to every static GTFS version the
watcher sees. Kafka is the transport for low-latency processing, not the system of record.

## Consequences

- Backfill and reprocessing read the archive with the same transformation code as the stream.
- Data collection can run for a few euro a month long before MSK and EMR exist.
- The local Kafka broker has no volume: losing it loses nothing.
- Every published number can be regenerated from raw bytes, which matters for credibility.
- Cost: one extra S3 PUT per poll (about 90,000 a month for two feeds every 60 s, well under $1).
- The poller must treat the S3 write as mandatory and the Kafka publish as best-effort with retry.
  If Kafka is down, the heartbeat records it and backfill closes the gap.
