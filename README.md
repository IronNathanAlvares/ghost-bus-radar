# Ghost Bus Radar

Independent, reproducible reliability numbers for Irish public transport: how often the
real-time board shows a bus that never comes, per route, stop and hour.

**Status:** Sprint 4 done. Every scheduled trip gets an outcome (ran, cancelled, vanished,
untracked, or no data when our own collection had a gap). Backfill from the raw archive equals
the live stream on every column of 10,528 trips, and a simulated outage produced 0 false ghosts.
First evening: 8.6% of fully collected bus trips untracked (95% CI 6.9-10.7), field check
pending. Collecting live NTA data since 15 November 2026. Sprint 5: collection runs in AWS since
18 November (Lambda, S3), and a nightly Glue job writes final outcomes to S3 Tables for Athena,
for a few dollars a month (ADR-0009, ADR-0010). Product: a nightly public JSON export counting only
days that passed the quality gates (ADR-0011), and a **"will my bus come?" API**,
`GET /v1/stops/{code}/departures`, with each bus's live status and a probability, served from the
archive without streaming at a gateway p95 of 323 ms (ADR-0012,
[measurement 06](docs/measurements/06-api.md)), and a **website** on CloudFront with the league table,
route pages, live departures at your stop and a methodology page (ADR-0013). The chance shown is
backtested every night on days it never saw ([measurement 08](docs/measurements/08-calibration.md)). How it all works, as diagrams:
[`docs/diagrams/`](docs/diagrams/README.md). See [`PLAN.md`](PLAN.md) for the design,
[`SPRINTS.md`](SPRINTS.md) for progress, [`HANDOFF.md`](HANDOFF.md) to pick it up cold.
Research and sources: [`docs/01-research.md`](docs/01-research.md). Decisions: [`docs/adr/`](docs/adr/).

Stack: Kafka 4 (Amazon MSK), Spark Structured Streaming on EMR Serverless, Apache Iceberg on
Amazon S3 Tables, Athena, Lambda + API Gateway, Power BI.

## Run it locally

Needs Docker and [uv](https://docs.astral.sh/uv/).

```bash
uv sync --all-packages
uv run pytest                       # unit tests, no Docker needed
```

```bash
docker compose -f local/docker-compose.yml up -d --wait
```

```bash
docker compose -f local/docker-compose.yml run --rm spark python -m ghostbus_jobs.smoke
```

Collect live data (needs `NTA_API_KEY` in `.env`; see `docs/02-nta-usage-policy.md`):

```bash
docker compose -f local/docker-compose.yml --profile collect up -d
```

```bash
docker compose -f local/docker-compose.yml logs -f poller
```

The poller makes one API call a minute (the policy limit), alternating TripUpdates and Vehicles,
with Alerts every 15 minutes. Snapshots go to `~/ghostbus-archive` (set `GHOSTBUS_ARCHIVE_DIR` to
move it), then to Kafka. Profile what has been collected with
`ARCHIVE_URI=file:///<path> uv run python -m ghostbus_ingest.feed_profile`.

Load the archived timetable into Iceberg as history, then measure how realtime joins it:

```bash
docker compose -f local/docker-compose.yml run --rm spark python -m ghostbus_jobs.timetable_load
```

```bash
docker compose -f local/docker-compose.yml run --rm spark python -m ghostbus_jobs.rt_match_report
```

Run the streaming pipeline (Kafka to bronze to silver, exactly once, 10-minute watermark):

```bash
docker compose -f local/docker-compose.yml --profile stream up -d
```

Prove exactly-once by killing it 20 times mid-flight and comparing with a clean run (~10 min):

```bash
docker compose -f local/docker-compose.yml run --rm spark pytest -m chaos -s
```

The smoke command sends 20 events through Kafka, Spark Structured Streaming and the Iceberg
streaming sink, then prints produce-to-queryable latency. Spark UI: http://localhost:4040 while a
job runs.

| Local service | Stands in for | Port |
|---|---|---|
| Kafka 4.1.1 (KRaft) | Amazon MSK | 9092 |
| Iceberg REST fixture 1.10 | S3 Tables Iceberg REST endpoint | 8181 |
| SeaweedFS S3 gateway | S3 | 8333 |
| `ghostbus/spark` (Spark 3.5.6, Iceberg 1.10) | EMR Serverless 7.12 | 4040 |

## Layout

| Path | What |
|---|---|
| `packages/core` | Pure logic shared by stream, batch and API: feeds and schedule, protobuf normaliser, archive layout, JSON Schemas. No Spark imports |
| `packages/ingest` | Poller, static watcher, archive, Kafka admin, feed profile |
| `packages/jobs` | Spark entry points |
| `packages/api` | "Will my bus come?" Lambda API (ADR-0012) |
| `local/` | Docker Compose stack and Spark image |
| `tests/` | Unit tests (host) and integration tests (inside the stack) |

## Data

Source: National Transport Authority (<https://www.nationaltransport.ie/>), GTFS and
GTFS-Realtime, licensed CC BY 4.0. Data is provided "as is"; the NTA is not responsible for
errors in it or in anything derived here. Usage follows the NTA developer usage policy
(`docs/02-nta-usage-policy.md`).
