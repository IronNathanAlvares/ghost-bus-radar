# ADR-0012: "Will my bus come?" API: live from the archive, no streaming

**Status:** accepted, 18 November 2026. Roadmap 2.3. Code: `ghostbus_core.departures` (pure),
`ghostbus_jobs.api_index` (Glue job `ghostbus-api-index`), `ghostbus_api.handler` (Lambda
`ghostbus-api` behind an API Gateway HTTP API).

## Context

A passenger at a stop wants to know, for each bus due in the next hour, whether it will really
come. The answer needs three things: the timetable for that stop, the live feed right now, and
how often buses like this one have actually come before.

Constraints:

- **One poller per API key.** The API must never call the NTA itself; the poller already archives
  a TripUpdates and a Vehicles snapshot every two minutes.
- **No streaming bill.** Kafka is a burst-mode extra (ADR-0009); nothing here may need it.
- **p95 under 500 ms**, and close to free at portfolio traffic.
- Measured on 18 November 2026: 770,000 departures a day at 10,116 stops (the busiest has 880); a
  TripUpdates snapshot is 220 KB compressed and decodes in 6 ms; route ids are stable across
  timetable versions, but at a version label change 63% of trip ids were regenerated.

## Decision

**1. A nightly stop index**, built by Glue right after `timetable-load` (in parallel with
`reconcile`). Every bus departure at every stop for today and tomorrow (local service days),
grouped into 256 shards by `crc32(stop code) % 256`, gzipped JSON, under
`s3://ghostbus-work-<account>/api/v1/builds/<build>/`. A pointer `api/v1/current.json` is written
**last**, so a reader always sees one complete build, never half of two (old builds expire after
3 days). About 260 PUTs a night.

**2. Live from the newest archived snapshots.** The Lambda lists the current hour's partition
of `raw/feed=trip_updates/` and `raw/feed=vehicles/`, reads the newest file of each, and keeps the
decoded result in memory keyed by the object key, so each snapshot is decoded once per container.
Live data older than 5 minutes is marked `stale`; older than 15 minutes it is not used at all.

**3. Matching a live trip to a scheduled one**: by `(trip_id, start_date)` first, then by the
natural key `(route_id, direction_id, start_date, start_time)`. The second is what keeps the
answer right after NTA publishes a timetable with regenerated trip ids (it publishes every night
around 23:20 UTC; the index is rebuilt at about 04:20 UTC).

**4. Status per departure**, from the live data: `cancelled` (trip CANCELED), `skips_stop` (stop
SKIPPED), `tracked` (the vehicle is reporting positions on this trip), `assigned` (a vehicle is
named but not reporting), `scheduled` (nothing live yet), `no_live_data` (live feed unavailable).
Expected time from the GTFS-Realtime delay at this stop, or propagated from the last earlier stop
with a prediction. A bus whose vehicle is already past the stop is dropped.

**5. Probability, model `heuristic-v1`.** The probability that the trip is RAN (tracked through
most of its route), from the route's published history in `data/v1/routes/{slug}.json`: the same
files the website reads, so the API and the site can never disagree, and only days that passed
the quality gates count. The live status rules out outcomes that are already impossible:

| Status | Outcomes ruled out | p |
|---|---|---|
| `cancelled`, `skips_stop` | all | 0 |
| `tracked` | GHOST, PHANTOM_LIVE, CANCELLED_EARLY | ran / (n - ghost - phantom - early) |
| `assigned` | GHOST, CANCELLED_EARLY | ran / (n - ghost - early) |
| `scheduled`, under 15 min away, live feed fresh | CANCELLED_EARLY | ran / (n - early) |
| `scheduled`, `no_live_data` | none | ran / n |

Counts come from the route's cell for that day type and trip start hour, shrunk toward the
route's overall rate with a prior worth 20 trips, so a thin cell can't produce 0% or 100%. With no
published history the probability is `null`, with the reason, never a made-up number. To make
this possible the export now publishes `ghost` and `phantom` separately as well as `untracked`
(an additive field: the v1 contract is unchanged for every existing reader).

**6. The edge.** HTTP API (cheaper and faster than a REST API), throttled to 10 requests/second
with bursts of 20, CORS open for GET only (the data is public), inputs validated against a strict
pattern. `GET /v1/stops/{code}/departures?minutes=60&limit=10`, response schema
`ghostbus.departures.v1`. Access logs record the route, status and latency, **not the caller's
IP address** (personal data under GDPR, and not needed).

## Consequences

- No new always-on cost: one Lambda, an HTTP API (about $1 per million requests), one more short
  Glue Flex job a night.
- The answer is at most one poll old (2 to 3 minutes), the same as the data. A real-time product
  would read the stream instead; this is the honest trade for no streaming bill.
- `heuristic-v1` is a model with stated assumptions, not a measured one: "tracked now" is treated
  like "reached a stop", and the trip start hour stands in for the hour at this stop. Roadmap 2.4
  measures it (Brier score and a reliability diagram on held-out days) before it is trusted.
- Buses only. Rail and Luas have no vehicle positions in the NTA feed (ADR-0008), so the model
  could say nothing useful about them. ADDED trips (not in the timetable) are not shown yet.
- If the nightly index fails, the current build still covers until the end of the next service
  day; after that the API answers 503 with the reason.
