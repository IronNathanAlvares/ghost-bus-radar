# Measurement 01: what the NTA GTFS-R v2 feeds contain

**Date:** 15 November 2026, Saturday evening (19:31 to 19:47 UTC). **Sprint:** S1.
**Status:** first profile, from the first 17 minutes of collection. The sprint's headline number,
fetch success over 72 hours, is still accruing; re-run the command below on 18 November and
replace the uptime table.

```bash
export ARCHIVE_URI=file:///C:/Users/<you>/ghostbus-archive
uv run python -m ghostbus_ingest.feed_profile --sample 40
```

The script reads only the archive (heartbeats, snapshots, the archived static zip), never the
API. Static version used for matching: `14FEE109-A0D9-4881-9F90-28D202C63489` (published
14 November 2026, valid to 14 November 2027).

## Collection uptime

| Feed | Scheduled calls | Collected | Success | 429s | p50 ms | p95 ms |
|---|---|---|---|---|---|---|
| trip_updates | 8 | 8 | 100% | 0 | 405 | 2,945 |
| vehicles | 8 | 8 | 100% | 0 | 224 | 634 |
| alerts | 1 | 1 | 100% | 0 | 553 | 553 |

Seventeen calls in seventeen minute slots, none throttled at one call per minute (ADR-0005).
"Collected" means HTTP 200, a decodable FeedMessage, and the raw bytes in the archive.

## Size, compression and freshness

| Feed | Raw KB (mean) | zstd-9 KB (mean) | Ratio | Entities min / mean / max | Header lag s p50 / p95 / max |
|---|---|---|---|---|---|
| trip_updates | 704 | 145 | 4.9x | 1,938 / 2,152 / 2,241 | 5 / 10 / 10 |
| vehicles | 67 | 20 | 3.4x | 774 / 792 / 807 | 4 / 8 / 8 |
| alerts | 18 | 6 | 3.2x | 26 | 252 |

- **Archive growth: about 111 MB a day, 3.3 GB a month.** In S3 Standard that is under
  $0.10 a month for the first month, and 720 calls a day per main feed is about 44,000 PUTs a
  month including heartbeats (about $0.22). Collection really is pocket change.
- **Header lag** (fetch time minus the feed header timestamp) is single-digit seconds for the two
  main feeds: snapshots are rebuilt continuously, so polling faster would see newer data. Alerts
  are rebuilt every few minutes.

## TripUpdates

| schedule_relationship | Per snapshot | Share |
|---|---|---|
| SCHEDULED | 2,078 | 96.5% |
| CANCELED | 38 | 1.8% |
| ADDED | 36 | 1.7% |

| Operator (static agency) | Trip updates per snapshot | Naming a vehicle |
|---|---|---|
| Bus Átha Cliath / Dublin Bus | 878 | 45% |
| Bus Éireann | 805 | 31% |
| Go-Ahead Ireland | 272 | 34% |
| Irish Rail (route id not in static, see below) | 58 | 0% |
| Iarnród Éireann / Irish Rail | 56 | 0% |
| LUAS | 48 | 0% |
| Bus Éireann Waterford | 35 | 19% |

- About **38 cancelled trips are live in every snapshot** on a Saturday evening. That is the
  raw material for `CANCELLED_EARLY` and `CANCELLED_LATE`.
- **A third of trip updates name a vehicle.** The rest are trips that have not started yet
  (predictions from the timetable) or trips with no tracked vehicle. Telling those apart needs
  the schedule (Sprint 2) and is the core of the ghost classifier.
- **Rail and Luas never name a vehicle**, and the Vehicles feed has no rail or Luas at all. This
  confirms the plan: ghost outcomes for bus only; rail and Luas get punctuality only.
- **Predictions are mostly `delay`** (94%), with absolute `time` mainly on ADDED trips.
- **Every non-ADDED `trip_id` matches the static file** (0.00% unmatched). Good news for
  Sprint 2's join, while the static version is current.
- **But 2.7% of `route_id`s do not match.** All are Irish Rail: the realtime feed says
  `DUB-GALWAY-I` where `trips.txt` puts the same `trip_id` on `DUB-GALWAY-O`. **Rule for Sprint 2:
  take the route from the static trip via `trip_id`, never from the realtime `route_id`.**
- **Entity ids are positions** (`T1`, `T2`, ...), reassigned every snapshot. Nothing may
  dedupe on them; records carry a `trip_key` instead.
- The per-trip `timestamp` equals the header timestamp, so it says when the snapshot was built,
  not when the trip was last observed.

## Vehicles

| Operator (static agency) | Vehicles per snapshot |
|---|---|
| Bus Átha Cliath / Dublin Bus | 406 |
| Bus Éireann | 271 |
| Go-Ahead Ireland | 108 |
| Bus Éireann Waterford | 7 |

- Every position is linked to a trip (100%), which makes "was this trip tracked?" a join, not a
  guess.
- **Position age at fetch: p50 24 s, p95 420 s, max 1,666 s.** Some vehicles stay in the feed
  for many minutes after their last report. **Rule for Sprint 4:** a position only counts as
  tracking if its own timestamp advances; a stale position repeated across snapshots is not
  evidence that the bus moved.

## What this changes

| Finding | Where it lands |
|---|---|
| One call a minute for the whole API, feeds alternate | ADR-0005, `feeds.feed_for_slot` |
| Route from static trip, not realtime `route_id` | Sprint 2 join rules |
| Stale vehicle positions persist for up to ~28 min | Sprint 4 tracking definition |
| No vehicles for rail and Luas | Ghost outcomes for bus only (as planned) |
| Entity ids are positional | `trip_key` on every record; silver dedupes on it (Sprint 3) |
