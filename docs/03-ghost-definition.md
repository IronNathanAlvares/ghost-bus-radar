# What counts as a ghost bus

This is the specification every number on the website rests on. The code is
`packages/core/src/ghostbus_core/outcomes.py` (pure, property-tested); this page is its reasoning.

## What we can and cannot see

We see what the NTA's real-time systems report: TripUpdates (predictions, cancellations, and
sometimes the vehicle assigned) and Vehicles (GPS positions), polled one feed per minute, so each
is refreshed every two minutes. We never see the bus itself. Three consequences:

- **"Not tracked" is not "did not run."** A bus with a broken tracker still carries passengers.
  Headlines say **untracked**, and the field check (`docs/04-field-check.md`) measures how often
  an untracked bus actually came.
- **Our own downtime must never look like a ghost.** Every API call writes a heartbeat; a trip
  whose evidence window overlaps a gap in our collection is `NO_DATA` unless we have positive
  evidence (a cancellation announced in time, or a bus tracked to the end).
- **Rail and Luas publish no vehicle positions**, so for them absence of a vehicle proves nothing.
  They are `OUT_OF_SCOPE` for outcomes and never counted.

## The outcomes

Every scheduled trip instance (from the timetable version that defined that day, ADR-0006) gets
exactly one outcome, decided **60 minutes after its scheduled end** (buses were observed running up
to 35 minutes late on the first evening of data). Rules are checked in this order:

| Outcome | Rule | What a passenger experienced |
|---|---|---|
| `OUT_OF_SCOPE` | Not a bus route (`route_type` 3) | (not measured) |
| `CANCELLED_EARLY` | `CANCELED` in TripUpdates at least **15 min** before scheduled departure, and Vehicles collected throughout (so we know it did not run anyway) | An honest cancellation, visible in the app in time |
| `CANCELLED_LATE` | `CANCELED` later than that, with both feeds collected from 30 min before departure | Already waiting, or on the way, when it was pulled |
| `RAN` | A position came within **250 m** of stops covering at least **60%** of the route (also if a cancellation was announced but it ran anyway) | A bus came |
| `VANISHED` | Tracked on the route (reached at least one stop), then lost before 60%, both feeds fully collected | The bus disappeared from the board mid-route |
| `PHANTOM_LIVE` | Not cancelled, never reached a stop, but a vehicle was assigned: named in TripUpdates, or reporting positions off the route | The board showed a live countdown to a bus that never came |
| `GHOST` | Not cancelled, no vehicle assigned or tracked, both feeds fully collected | The bus was simply not there |
| `NO_DATA` | The evidence for any outcome above could have been hidden by a gap in our collection | **Excluded from every rate** |

"Untracked" in headlines = `GHOST` + `PHANTOM_LIVE`.

### Why these choices

- **Realtime ids can lag a timetable change.** NTA's live feed kept the previous zip's trip ids
  for 76 minutes after a publish, so a realtime id not in the live version is looked up in the
  previous one for 3 hours after each publish. Without this, 24 trips on 15 November 2026 were
  called ghosts or phantoms and 92 "vanished" although they ran (rules version 5).
- **Progress from geometry.** NTA vehicle positions carry no stop sequence or stop id (0% of rows
  on 15 November 2026), so progress is the furthest stop on the trip that any position came within
  250 m of, as a share of the trip's stops.
- **`PHANTOM_LIVE` includes a reporting vehicle that never reached the route.** Found in the
  first evening of data: bus 6846, assigned to a Dundalk trip, reported every two minutes from one
  spot 1 km from the first stop for the whole trip. It never served a stop; the board showed it live.
- **`VANISHED` needs both feeds covered**, not just Vehicles: a mid-route cancellation published
  during a TripUpdates gap would also explain the bus disappearing. Found by a property test.
- **A cancellation needs the vehicle feed covered too.** Found by the outage demo: a trip
  cancelled early that ran anyway was tracked through its stops (RAN); with a simulated outage
  hiding its positions it became CANCELLED_EARLY. A gap leaves "did it run?" open, so it is NO_DATA.
- **Only a bus tracked to the end survives our gaps; absence of evidence never does.** A property test checks, over random
  evidence, that a collection gap can never produce `GHOST`, `PHANTOM_LIVE` or `VANISHED`, and that
  closing a gap only ever turns `NO_DATA` into a specific outcome.
- **Evidence stops at the trip's close time** (scheduled end plus 60 minutes). A late bus stays
  in the feeds after that, and counting it up to "now" made the same trip's evidence depend on
  when it was computed. With the bound, a closed trip's outcome is the same from the stream and
  from the archive, today or next year.
- **Collection gap** = more than 5 minutes between two collected snapshots of a feed (normal
  spacing is 2 to 3 minutes). The window that must be covered runs from 30 minutes before scheduled
  departure to 30 minutes after scheduled end.

## Live versus final

- **Provisional** outcomes come from the streaming silver tables a few minutes after each trip
  closes (`trip_state`). They can miss rows the stream's watermark dropped as late.
- **Final** outcomes come from the raw archive (`reconcile`, nightly for the day before, and the
  backfill path for any range). Final always replaces provisional, never the other way round.
- Every row stores its evidence (snapshot counts, cancellation time, positions, progress, gap
  flags), the reason in words, and the threshold version, so any number can be re-derived and the
  thresholds can be varied without re-reading raw data (`outcome_report` sensitivity table).

## How numbers are published

- Rates use only bus trips whose whole evidence window was collected (`in_rates`), always with
  the sample size and a 95% Wilson interval. The sample is chosen by coverage, never by outcome.
  A bus tracked to its end keeps its `RAN` across a gap, but a bus that did not run becomes
  `NO_DATA`, so counting every decided trip would overstate service. On the first evening that
  bias was 94% ran against 87%.
- Routes with fewer than 30 rated trips in a window are shown but not ranked.
- The untracked rate is always shown next to the field-check result: the share of "untracked"
  trips that were seen to arrive.
- Thresholds are defaults, not truths: the sensitivity table shows how much each rate moves when
  they move.
