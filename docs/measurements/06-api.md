# Measurement 06: the "will my bus come?" API

**Date:** 18 November 2026. **Roadmap:** 2.3. Design: ADR-0012. Region `eu-north-1`, Lambda
`ghostbus-api` (Python 3.11, arm64, 512 MB) behind an HTTP API.

## 1. Sizing that shaped the design

Measured before building, from the lake (Athena) and the archive:

| What | Value | Consequence |
|---|---|---|
| Departures a day, all stops | 770,000 at 10,116 stops; busiest stop 880, 99th percentile 415 | Too many for one file per request; too many stops for one file each |
| TripUpdates snapshot | 220 KB compressed, 1.1 MB raw, 3,555 trips, **decodes in 6 ms** | Reading the newest archived snapshot per request is affordable: no stream needed |
| Vehicles snapshot | 31 KB compressed, 1,281 vehicles, 2 ms | |
| Route ids across timetable versions | 402 of 402 unchanged | Safe to match on |
| Trip ids across the 16 November label change | 95,255 of 254,953 kept (37%) | Matching by trip id alone would lose 63% of trips after a republish; the API falls back to route, direction, date and start time |

## 2. The nightly stop index

| | Run 1 | Run 2 |
|---|---|---|
| Glue `ghostbus-api-index` (Flex, 2 × G.1X) | 208 s | 171 s |
| Bus departures indexed (18 and 19 Nov) | 1,390,151 | 1,390,151 |
| Stops with a departure | 9,735 | 9,735 |
| Files | 256 shards + pointer | same |
| Size | 19.4 MB in all; average shard 76 KB, largest 117 KB (gzipped) | same |

The second run is the same data with `valid_until` added to the pointer (section 4).

## 3. Latency

200 sequential requests over 40 stops (the 20 with the most routes and 20 random ones), paced
under the throttle. "Gateway" is API Gateway's own `responseLatency` from the access logs; "handler"
is the Lambda's own timing of the request.

| Version | Handler p50 | Handler p95 | Gateway p50 | Gateway p95 | Gateway p99 |
|---|---|---|---|---|---|
| First deploy | 599 ms | 1,029 ms | | | |
| After the fixes below | **0.5 ms** | **186 ms** | **17 ms** | **323 ms** | 498 ms |

Target: p95 under 500 ms. **Met.** A cold start adds 0.22 s of initialisation; the slowest request
in the run (2.1 s) was the first one in a new container.

From a laptop in Ireland the client sees about 300 ms more (p50 317 ms), almost all of it a new
TLS connection to Stockholm for every request. A browser keeps the connection open, and the 30-second
cache header lets CloudFront answer repeats (roadmap 2.2).

**What made the first deploy slow, and the fixes** (found by timing each stage in the log line):

1. **Every request re-parsed the whole live feed.** Building the live view turned all 3,555 trips
   and every one of their stop updates into Python objects, about 0.5 s on Lambda, for a stop that
   needs about 10 trips. Now the view is built once per snapshot (about once a minute) and a trip's
   stop updates are parsed only when a stop asks for that trip.
2. **Route histories were fetched one after another.** A busy stop has about 20 routes; 20 S3 round
   trips in sequence. Now the uncached ones are fetched in parallel (8 threads).

After both, a warm request reads nothing from S3 for up to 20 seconds. The p95 is the first read of
a stop's shard (p95 181 ms for that stage).

## 4. Correctness checks on the live API

- 200 of 200 requests answered 200; the live feed was `fresh` in every answer.
- Status mix over 1,355 departures shown: 957 tracked, 378 scheduled, 10 assigned, 10 cancelled.
- Probabilities: `null` for every running bus, with the reason "no published history for this route
  yet", because no service day has passed the quality gates (15 to 17 November failed completeness;
  the first full day is 19 November). Cancelled buses and skipped stops show 0.
- Refused as designed: a 999-minute window (400), an unknown stop (404), an unknown route (404),
  POST (404), a path-traversal attempt (`abc/../x` is normalised by the gateway, then validated: 404).
- CORS: `access-control-allow-origin: *` on GET.
- **A design fix found while checking the first build:** the pointer said the index lasted until its
  last departure, 07:19 on 20 November, because of one overnight coach. A failed nightly run would
  then have shown near-empty boards from 04:00 to 07:19 instead of an honest error. The pointer now
  carries `valid_until`, the start of the first service day not covered (04:00 local, two days on),
  and the API answers 503 `index_expired` from then.

## 5. Cost

At portfolio traffic, effectively nothing: HTTP API requests about $1 per million, Lambda within the
always-free allowance, S3 reads of a few hundred KB per new container, about 260 PUTs a night for the
index, and one more Flex Glue run of about 3 minutes a night (about $0.03 at 2 × G.1X).
