# Measurement 10: how accurate is the countdown?

**Date:** 19 November 2026. **Roadmap:** 1.4. **Design:** ADR-0016. Glue job `ghostbus-accuracy`
run once for 16 to 18 November: 547 s (about 3 minutes a day). Nightly from now on, after
`reconcile`.

## 1. What the feeds allow

- TripUpdates carry per-stop predictions: of 1,251 trips in one late-evening snapshot, 600 had
  five or more stops updated. Arrival and departure delays are both common; absolute times rare.
- Vehicles carry only positions: 0 of 609 had a stop or progress field. A tenth of positions were
  over six minutes old when fetched, so each position's own timestamp is used.
- Each feed is fetched every two minutes (alternating), which sets the time resolution.

## 2. Coverage

| | |
|---|---|
| Trips scored (ran, fully collected, buses) | about 12,100 |
| Countdowns scored | **5,844,667** |
| Stops with a reconstructed arrival time | 87.2% (all but each trip's first stop) |

## 3. Results, all operators

Error = actual arrival − predicted (positive: the bus came later than the board said).

| Board says | Countdowns | Typical error | 8 in 10 between | Mean | Within 1 min | Within 2 min (95% interval) | Timetable within 2 min |
|---|---|---|---|---|---|---|---|
| 0–2 min | 365,404 | +0.3 | −0.8 to +2.3 | +0.9 | 71% | **88%** (88–89%) | 32% |
| 2–5 min | 594,444 | +0.3 | −0.8 to +3.3 | +1.0 | 55% | **80%** (79–81%) | 33% |
| 5–10 min | 984,364 | +0.8 | −1.3 to +4.3 | +1.3 | 41% | **67%** (67–68%) | 33% |
| 10–15 min | 985,447 | +1.3 | −1.8 to +5.3 | +1.7 | 32% | **57%** (56–57%) | 33% |
| 15–20 min | 977,736 | +1.3 | −2.3 to +6.3 | +2.0 | 28% | **50%** (49–51%) | 34% |
| 20–30 min | 1,937,272 | +1.8 | −2.3 to +7.8 | +2.4 | 24% | **44%** (43–45%) | 34% |

The timetable alone, at the same moments: typical error +3.3 minutes, mean absolute error about
5.6 minutes, within 2 minutes about a third of the time, whatever the lead.

By operator, when the board says 5–10 minutes:

| Operator | Countdowns | Typical error | Mean absolute error | Within 2 min | Timetable's mean absolute error |
|---|---|---|---|---|---|
| Dublin Bus | 574,654 | +0.8 | 1.7 min | **73%** | 5.0 min |
| Go-Ahead Ireland | 160,398 | +0.8 | 2.3 min | 63% | 5.7 min |
| Bus Éireann | 233,696 | +1.3 | 2.7 min | 58% | 7.1 min |

## 4. What it says

1. **The countdown is far better than the timetable**, at every lead time and for every operator:
   88% within 2 minutes when it says "due", against a third for the timetable.
2. **It is optimistic, and more so further out.** Buses arrive later than shown, by about
   1 minute on average when the board says 0–2 minutes and 2.4 minutes at 20–30. A countdown of
   "20 minutes" is better read as "about 22". The likely cause: the feed carries today's delay
   forward unchanged, while real delays keep growing along a route.
3. **Accuracy falls off fast with distance.** Within 2 minutes 88% of the time at 0–2 minutes ahead,
   44% at 20–30: a passenger deciding whether to run for a bus 15 minutes away is guessing more than
   the board suggests.

## 5. Limits

- Arrival times are reconstructed from positions two minutes apart (constant speed between them).
  The property test bounds the error by the gap; the planned field check (a stopwatch at real stops)
  can measure it. The same reconstruction serves the real-time and timetable errors, so the
  comparison between them is fair either way.
- **A bug found on the way:** a position exactly at a stop and the stop itself are measured along
  the route by different sums, and the last-digit float difference put such positions just short of
  the stop. A bus waiting at a stop was then timed when it *left*. The integration test caught it
  (18 minutes off on a synthetic dwell); positions within a metre of a stop now count as at it, and a
  unit test on a diagonal route keeps it fixed.
- Three days so far; the intervals use the number of trips, not countdowns (predictions of one trip
  are not independent), so they are conservative.
