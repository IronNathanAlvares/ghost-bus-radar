# Measurement 04: trip outcomes, backfill and the first rates

**Date:** 15 November 2026. **Sprint:** S4. Design: ADR-0008. Rules:
[`docs/03-ghost-definition.md`](../03-ghost-definition.md) (rules version 4, default thresholds,
`rules4-p0.6-c15-r250-g5-w30.30-x60`).

All runs below use the live collection, which started at **19:31 UTC on Sunday 15 November**,
with the data clock at **23:15 UTC**. Dublin is on UTC in November.

## 1. Backfill equals the stream, and an outage never makes a ghost

```bash
docker compose -f local/docker-compose.yml run --rm spark python -m ghostbus_jobs.backfill_demo --day 2026-11-15 --hole 20:45 21:45
```

The demo decides the same closed trips three ways:

- **A** from the streaming silver tables as they are.
- **B** from the silver tables with a simulated one-hour outage, 20:45 to 21:45 UTC. Every row
  and heartbeat fetched in that hour is removed, as if the collector or stream had been down.
- **C** from the raw archive alone. This is what `reconcile` and any backfill use.

| Outcome | A uninterrupted | B with outage | C backfill from archive |
|---|---|---|---|
| OUT_OF_SCOPE | 882 | 882 | 882 |
| CANCELLED_EARLY | 9 | 0 | 9 |
| CANCELLED_LATE | 4 | 0 | 4 |
| RAN | 1,749 | 1,417 | 1,749 |
| VANISHED | 21 | 0 | 21 |
| PHANTOM_LIVE | 6 | 0 | 6 |
| GHOST | 64 | 0 | 64 |
| NO_DATA | 7,793 | 8,229 | 7,793 |

- **Backfill equals uninterrupted, row for row. 0 of 10,528 rows differ** in outcome, reason or
  any evidence column (snapshot count, cancellation time, vehicle flag, positions, first and last
  position, progress, both gap flags).
- **The outage changed 436 trips, all of them to `NO_DATA`. None became a ghost, a phantom or a
  vanished bus.** Every non-`RAN` outcome disappeared in B because the one-hour hole overlaps
  almost every trip window that was fully collected on this first evening. The 1,417 trips that
  survived as `RAN` were tracked to their end outside the hole.
- A is computed exactly as the live `trip_state` service computes provisional outcomes: the same
  function over silver, with the data clock in place of the wall clock. So A = C also means that
  on this day **provisional equals final**. The 10-minute watermark dropped no row that mattered.

Then `reconcile --from 2026-11-15 --to 2026-11-15` wrote 10,528 final outcomes, replacing every
provisional one.

## 2. Findings that changed the code

### 2.1 Test data in the live tables, and evidence that depended on "now"

The first version of the demo compared outcomes only, and found 0 differences. Comparing every
evidence column instead found **15 rows where backfill and stream disagreed**, with two causes.

**Test data had leaked into the live tables.** The collector integration test published its
fixture (two trip updates, one cancelled) to the live Kafka topics on the local stack. Running it
during Sprints 1 to 4 left one fixture fetch in silver and **four fake "collected" heartbeats**.
A fake heartbeat is worse than a fake row: it can hide a real collection gap.

- The test now publishes to its own prefixed topics (`KafkaPublisher(topic_prefix=...)`,
  `ensure_topics(prefix=...)`) and deletes them afterwards.
- The leaked rows were removed from bronze and all four silver tables. The check is a new job,
  `ghostbus_jobs.lake_audit`: every `fetch_id` in the lake must exist in the raw archive. It now
  passes for all five tables, and `--delete` removes any offenders from every layer.
- Removing them showed a second problem. Iceberg's streaming source **stops on any snapshot that
  deletes or rewrites rows**, so a data correction would have crashed the live silver and
  `trip_state` queries. Readers now skip those snapshots, and a correction is applied to every
  layer explicitly. An integration test deletes from bronze and silver mid-stream and checks that
  new data still flows. Without the fix, the test fails with `Cannot process overwrite snapshot`.

**Evidence was counted up to "now".** A late-running bus is still in the feeds after its trip
closes, and the archive is one poll ahead of the stream, so the archive counted one more
snapshot. The same trip got different evidence depending on when, and from where, it was
computed.

- Evidence now counts only data fetched by the trip's close time (scheduled end plus 60 min), so
  a trip's evidence is fixed once it closes.
- An integration test adds a vehicle sighting two hours after a ghost trip closed. Without the
  fix, the ghost turns into `PHANTOM_LIVE`.
- The rules version went to 4.

### 2.2 Survivorship through gaps inflated the ran rate

The first report computed rates over every decided trip and gave **94.4% ran**. Splitting by
coverage showed why that was wrong:

| Outcome | Whole window collected | Decided through a collection gap |
|---|---|---|
| RAN | 709 | **1,040** |
| VANISHED, PHANTOM_LIVE, GHOST, cancellations | 104 | 0 |

Both rules are correct for a single trip. A bus tracked to its end stands even when our
collection had a gap; a bus that did not run is `NO_DATA` under the same gap. Across a gap,
then, only good news survives, and a rate over all decided trips overstates service. On this
evening most trips started before collection did, so 3 in 5 `RAN` trips were decided that way.

**Fix:** rates use only trips whose whole evidence window was collected
(`ghostbus_core.outcomes.in_rates`). That sample is chosen by coverage, never by outcome.

- A property test checks that every such trip gets a rated outcome.
- The `RAN`-through-a-gap trips keep their outcome (a page can still say "this bus ran") but are
  not in any rate.
- Ran rate on the same data: **87.2%, not 94.4%.**

## 3. First rates (one Sunday evening)

```bash
docker compose -f local/docker-compose.yml run --rm spark python -m ghostbus_jobs.outcome_report --from 2026-11-15 --to 2026-11-15
```

813 bus trips had their whole window collected (scheduled departures 20:03 to 22:00 UTC). Rates
with 95% Wilson intervals:

| Operator | Trips in rates | Ran | Untracked (ghost + phantom) | Vanished | Cancelled late | Cancelled early |
|---|---|---|---|---|---|---|
| All buses | 813 | 87.2% (84.7-89.3) | 8.6% (6.9-10.7) | 2.6% (1.7-3.9) | 0.5% (0.2-1.3) | 1.1% (0.6-2.1) |
| Dublin Bus | 354 | 91.2% (87.8-93.8) | 4.5% (2.8-7.2) | 3.7% (2.2-6.2) | 0.3% (0.0-1.6) | 0.3% (0.0-1.6) |
| Bus Éireann | 287 | 77.7% (72.5-82.1) | 16.4% (12.5-21.1) | 2.4% (1.2-4.9) | 0.7% (0.2-2.5) | 2.8% (1.4-5.4) |
| Go-Ahead Ireland | 145 | 96.6% (92.2-98.5) | 2.1% (0.7-5.9) | 0.7% (0.1-3.8) | 0.7% (0.1-3.8) | 0.0% (0.0-2.6) |
| Bus Éireann Waterford | 23 | 82.6% (62.9-93.0) | 17.4% (7.0-37.1) | 0.0% (0.0-14.3) | 0.0% (0.0-14.3) | 0.0% (0.0-14.3) |
| Go-Ahead Ireland (commuter) | 4 | 100.0% (51.0-100.0) | 0.0% (0.0-49.0) | 0.0% (0.0-49.0) | 0.0% (0.0-49.0) | 0.0% (0.0-49.0) |

The 70 untracked trips break down as follows:

| What the feeds showed | Trips |
|---|---|
| Listed in TripUpdates with no vehicle | 44 |
| Absent from TripUpdates entirely | 20 |
| A vehicle reported positions but never reached a stop | 5 |
| A vehicle was named in TripUpdates but never reported a position | 1 |

**Cancelled, but the bus came.** 31 trips in the sample carried a cancellation notice, and 18
of them were tracked through at least 60% of their stops.

- For 14 of the 18, TripUpdates said `CANCELED` in every snapshot until the trip closed. Each was
  clearly running: the 21:00 route 354, for example, was tracked at 22 different spots over 48
  minutes.
- They count as `RAN`, as the rules intend. But "told it was cancelled, the bus came" is a
  passenger harm in its own right. It is a candidate for its own published rate once there is
  more than one evening of data.

**Read these as a demonstration of the method, not as findings about operators:**

- The data is one Sunday evening, about two hours of departures. No route has 30 trips in rates
  yet, so no route is ranked (0 of 257).
- "Untracked" is not "did not run". The Bus Éireann figure (16%, against 4.5% for Dublin Bus)
  could be broken trackers on regional coaches as easily as missing buses. The field check
  (`docs/04-field-check.md`) measures which, and it is not done yet.
- Evening service at the end of a weekend is not representative of a weekday peak.

## 4. Sensitivity

Reclassifying the stored evidence of the same 813 trips with other thresholds:

| RAN threshold | Untracked | Vanished | Cancelled late |
|---|---|---|---|
| 40% | 8.6% (6.9-10.7) | 2.0% (1.2-3.2) | 0.2% (0.1-0.9) |
| 50% | 8.6% (6.9-10.7) | 2.3% (1.5-3.6) | 0.5% (0.2-1.3) |
| **60% (default)** | **8.6% (6.9-10.7)** | **2.6% (1.7-3.9)** | **0.5% (0.2-1.3)** |
| 70% | 8.6% (6.9-10.7) | 3.1% (2.1-4.5) | 0.5% (0.2-1.3) |
| 80% | 8.6% (6.9-10.7) | 3.7% (2.6-5.2) | 0.5% (0.2-1.3) |

- **The untracked rate does not move at all.** It depends on whether a bus reached any stop, not
  on how far it got, so the headline number is robust to the most debatable threshold.
- Vanished moves from 2.0% to 3.7% across the range. That number is threshold-sensitive and is
  published with its threshold.
- The early-cancel notice barely matters. Varying it from 5 to 30 minutes moves one trip between
  late and early: cancelled late is 0.4% at 5 minutes and 0.5% from 10 minutes up.
- Stop radius and gap length change the evidence itself. Their sensitivity needs a `reconcile`
  run with other parameters (Sprint 6, with more data).

## 5. Rule changes this sprint, each found by a test or the data

| Version | Change | Found by |
|---|---|---|
| 1 | Initial rules | The definition |
| 2 | `VANISHED` needs TripUpdates covered too: a mid-route cancellation could be in the gap | Hypothesis property test |
| 2 | A vehicle that reports positions but never reaches a stop is `PHANTOM_LIVE` | Live data: bus 6846 sat 1 km from its first stop for a whole Dundalk trip |
| 3 | A cancellation with a vehicle-feed gap is `NO_DATA` | Outage demo: a cancelled trip that ran became `CANCELLED_EARLY` when the hole hid its positions |
| 4 | Evidence counts only data fetched by the trip's close time | Full-row backfill comparison (section 2.1) |
| (publishing) | Rates use fully collected trips only | Outcome report (section 2.2) |

## Still open

- **Field check.** About 50 observed departures, to measure how often "untracked" buses actually
  came. This needs a person at a bus stop.
- **A full weekday of outcomes**, so routes can be ranked. Monday 16 November is the first
  possible day if the collector is left running; it can be decided after 01:00 on the 17th.
- Final outcomes are written by running `reconcile` by hand. Sprint 5 schedules it nightly.
- The leaked fixture messages are still in the local Kafka topics until their 7-day retention
  expires. The streamer's checkpoint is past them, but a streamer started with a fresh checkpoint
  before 22 November would read them again. `lake_audit` would catch that; Sprint 5 runs it
  nightly after `reconcile`.
