# ADR-0016: How accurate is the countdown? Predicted vs actual arrival at stops

**Status:** accepted, 19 November 2026. Roadmap 1.4. Code: `ghostbus_core.accuracy` (pure),
`ghostbus_jobs.accuracy` (Glue job `ghostbus-accuracy`).

## Context

Ghost Bus Radar so far answers "does the bus come?". Passengers also ask "when the board says 5
minutes, how long is it really?". The NTA's TripUpdates carry a predicted delay per upcoming stop,
which every app turns into a countdown; the archive keeps every snapshot. The Vehicles feed has
positions and their timestamps but **no stop or progress fields** (checked 19 November: 0 of 609
vehicles had `current_stop_sequence`, `stop_id` or `current_status`), and a tenth of positions are
more than six minutes old when fetched. Each feed is fetched every two minutes.

## Decision

- **Actual passage time, reconstructed from positions.** For each trip that ran, every position
  (at its own timestamp, not the fetch time) is projected onto the chain of the trip's stops,
  searching forward from the last match so loops and backtracks can't confuse it, and dropping
  fixes more than 150 m from the route. The time the bus passed a stop is interpolated between the
  two consecutive fixes either side of it, **only when those fixes are at most 4 minutes apart**;
  otherwise that stop gets no time. Constant speed between fixes is the assumption; a property test
  shows the error never exceeds the gap. A fix within a metre of a stop counts as at it (float
  rounding otherwise timed a waiting bus when it left). **A trip's first stop gets no time:** buses
  wait there, so "when it passed" is ambiguous and its countdown is about departure.
- **The prediction, as a board would show it.** At each archived TripUpdates snapshot, for every
  stop the bus had not yet passed: the predicted time is the stop's own update (absolute time, or
  scheduled + delay), else the delay of the last earlier updated stop carried forward, the same
  rule as the API (ADR-0012). Lead = predicted time − snapshot time ("the board says *lead*").
- **Error = actual − predicted** (positive: the bus came later than the board said), grouped by
  lead (0–2, 2–5, 5–10, 10–15, 15–20, 20–30 minutes) and operator. **Baseline: the timetable**
  (actual − scheduled) at exactly the same moments, so "does real-time beat the timetable?" is
  answered on the same predictions.
- **Statistics that survive aggregation.** Per day, operator and lead bucket: counts, sum and sum
  of absolute errors, shares within ±1 and ±2 minutes, the number of trips, and a 30-second
  histogram of errors from −10 to +30 minutes (both real-time and timetable). Medians and the
  10th/90th percentiles over any window come from merged histograms. Stored in
  `gold.prediction_accuracy` (day replaced on rerun); the last 28 days published as
  `data/v1/accuracy.json` and shown on the home page.
- **Honest intervals.** Predictions of one trip are strongly correlated (one late bus is late at
  every stop), so the 95% intervals on shares use the **number of trips**, not the number of
  predictions, as the sample size. Conservative on purpose.
- **Which trips:** fully collected bus trips that ran (`in_rates`, outcome RAN), in the API's scope.

## Consequences

- A new passenger-facing number, from data already collected, at the cost of one more Flex run
  a night (in parallel with `quality` and `calibration`).
- The reconstruction has an error of its own (a bus that dwells at a stop between two fixes looks
  as if it passed earlier). It is the same for real-time and timetable, so the comparison between
  them stays fair; the planned field check (stopwatch at real stops) can measure it directly.
- Every other stop is timed on arrival (the first fix at the stop), which is what a waiting
  passenger cares about. First result: measurement 10.
