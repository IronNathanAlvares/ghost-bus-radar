# ADR-0004: The local stack speaks the same protocols as AWS

**Status:** accepted, 15 November 2026

## Context

Bugs that only appear in the cloud are expensive to find when the cloud costs money per hour.
S3 Tables exposes the standard Iceberg REST catalog API, so a local REST catalog can stand in
for it without code branches. MinIO stopped publishing free container images in 2025, so the
usual local S3 choice is gone.

## Decision

| Cloud | Local | Same protocol |
|---|---|---|
| S3 Tables REST endpoint (SigV4) | `apache/iceberg-rest-fixture:1.10.0` | Iceberg REST |
| S3 | SeaweedFS 4.51 S3 gateway | S3 API, path-style |
| MSK, Kafka 4.1 | `apache/kafka:4.1.1`, KRaft | Kafka |
| EMR 7.12 (Spark 3.5.6, Iceberg 1.10) | `ghostbus/spark` image, same versions | |

Only `ghostbus_core.lake.spark_catalog_conf` knows which environment it is in. Python is 3.11
everywhere; EMR Serverless will get a custom image or packaged virtualenv with 3.11 (its default
is older), decided in S5.

## Consequences

- The smoke test exercises the real Kafka source and the real Iceberg streaming sink.
- SigV4 signing and Lake Formation permissions are not exercised locally; S5 needs its own checks.
- The REST fixture keeps its catalog in memory: `docker compose down` forgets table metadata
  even though the files remain. Acceptable for a dev loop; use `down -v` to keep the two in step.
