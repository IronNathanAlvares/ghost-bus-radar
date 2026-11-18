# Measurement 02: realtime against the versioned timetable

**Date:** 15 November 2026. **Sprint:** S2. **Commands:**

```bash
docker compose -f local/docker-compose.yml run --rm spark python -m ghostbus_jobs.timetable_load --from 2026-11-14 --to 2026-11-22
docker compose -f local/docker-compose.yml run --rm spark python -m ghostbus_jobs.rt_match_report
```

Input: one archived static version and every realtime snapshot collected so far
(19:31 to 20:05 UTC on Sunday 15 November: 17 TripUpdates and 18 Vehicles snapshots).

## The timetable as loaded

| Feed version | Valid from (UTC) | Feed dates |
|---|---|---|
| `14FEE109-A0D9-4881-9F90-28D202C63489` | 2026-11-14 23:09 | 2026-11-14 to 2027-11-14 |

402 routes and 10,270 stops (SCD2, all current), 190,466 trips, 7,427,633 stop times, no route
natural-key collisions once rail keys include the long name. Load time locally: about 40 s.

| Service date | Trip instances | With duplicate index |
|---|---|---|
| Sun 15 Nov | 12,185 | 0 |
| Mon 16 Nov | 19,762 | 31 |
| Tue 17 Nov | 19,767 | 31 |
| Wed 18 Nov | 19,770 | 29 |
| Thu 19 Nov | 19,767 | 29 |
| Fri 20 Nov | 19,780 | 30 |
| Sat 21 Nov | 16,145 | 0 |
| Sun 22 Nov | 12,184 | 0 |

14 November has no schedule: it started before the first version we archived was published
(ADR-0006, version live at 04:00 local defines the day). The latest scheduled end is 08:38 UTC
the following morning (GTFS time 32:38, an overnight service).

## The Sprint 2 measure: unmatched realtime trip ids

| Check | TripUpdates | Vehicles |
|---|---|---|
| Trips observed | 35,521 | 13,823 |
| With a `trip_id` (not ADDED) | 34,907 (98.27%) | 13,691 (99.05%) |
| **`trip_id` not in the live version (unmatched)** | **0 (0.00%)** | **0 (0.00%)** |
| Realtime `start_time` equals scheduled first departure | 100.00% | 100.00% |
| Resolved to a trip instance on its `start_date` | 100.00% | 100.00% |
| ADDED trips | 614 | 132 |
| ADDED with a resolvable `route_id` | 100.00% | 100.00% |

Every agency is at 0.00% unmatched, including Irish Rail, whose realtime `route_id`s do not
always match the static file (measurement 01). That is harmless now: routes are taken from the
static trip via `trip_id`.

## Reading it

- The join that every later sprint depends on (realtime trip to scheduled trip instance) works
  for every trip observed, and the realtime `start_time` agreeing with the schedule's first
  departure in 100% of cases says it is joining the *right* trip, not just *a* trip.
- This is with a single static version, a day after it was published. **The number that matters
  is the unmatched rate around a version change**, when NTA's realtime ids switch over. The
  static watcher checks hourly; when the next version arrives, re-run both commands and add a
  "transition" section here. Until then this measure is necessary, not sufficient.
- ADDED trips (1.7%) carry no `trip_id` and are, by definition, not in the timetable. They will
  be reported as extra service in Sprint 4, never as ghosts.

## Transition: realtime ids around timetable changes (18 November 2026)

`rt_match_report` over every archived snapshot (15 to 18 November), with four loaded versions:
14 Nov 23:09 (`14FEE109...-f72084e2`), 15 Nov 23:09 (same label, `-5a3116d6`), 16 Nov 23:20
(`03E379CA...`) and 17 Nov 23:27 (`E9368697...`).

| Feed | Trips with a `trip_id` | Unmatched in the live version | Resolved to a trip instance |
|---|---|---|---|
| trip_updates | 1,633,371 | **15,709 (0.96%)** | 99.14% |
| vehicles | 651,441 | **9,008 (1.38%)** | 99.09% |

- **Every unmatched id is Dublin Bus, and every one falls in one 75-minute window:** 23:10 on 15 Nov
  to 00:25 on 16 Nov, right after NTA published the same-label republish `5a3116d6`. That zip gave
  Dublin Bus trips new ids, but **the realtime feed kept the previous zip's ids for about 76
  minutes**; all 15,709 unmatched trip updates are found in the previous version.
- The two real timetable changes (16 and 17 November) switched over with **0 unmatched**: there,
  realtime ids changed at, or before, the publish time.
- Outside that window the unmatched rate is 0.00%, as in Sprint 2.

**Consequence and fix (roadmap 1.0):** the matcher assumes realtime ids switch exactly when the
new zip is published (`Last-Modified`). In this window, realtime records were not attached to
their trips, so late-evening trips on 15 November could be judged without their evidence (risk
of false `GHOST`). Fix: if a `trip_id` is not in the live version, try the previous version for a
few hours after each publish; re-run `reconcile` for 15 and 16 November and record how many
outcomes change.

