# ADR-0014: Calibrating "will my bus come?": a walk-forward backtest on the archive

**Status:** accepted, 18 November 2026. Roadmap 2.4. Code: `ghostbus_core.calibration` (pure
scoring), `ghostbus_jobs.calibration` (Glue job `ghostbus-calibration`).

## Context

The API shows "a 94% chance this bus comes" (ADR-0012). That number is a model with stated
assumptions (`heuristic-v1`), and a probability is only useful if it is *calibrated*: of all the
buses given about 90%, about 90% should come. Nothing checked that yet.

The raw archive holds every live snapshot, and the lake holds every trip's final outcome. So the
model can be scored on what it *would have said* at the time, against what then happened, without
waiting for real users.

## Decision

- **Walk-forward, never peeking.** For a test day *d*, the route history is built from the 28 days
  before *d* only, through the same path as production (cells, then `export.route_doc`, then
  `departures.history_from_route_doc`). The test day's own outcomes never inform its predictions.
- **The live status as it was.** For every fully collected bus trip on *d* (`in_rates`, the same
  trips the published rates use), the model is asked at **30, 15, 5 and 1 minutes before the
  scheduled start**. The archived TripUpdates and Vehicles snapshots at that moment are replayed
  through the production matcher (`attach_instance`, with the timetable-version fallback) to get
  the status the API would have shown: `cancelled`, `tracked`, `assigned`, `scheduled`, or
  `no_live_data` when the newest snapshot was over 15 minutes old. Then `departures.probability`
  gives the number, with the same freshness rule (5 minutes) as the API.
- **The outcome to predict** is RAN (y = 1) or anything else (y = 0), as the API defines it.
- **Scores**, by lead time and by live status, over the last 28 test days:
  - **Brier score**: the mean squared gap between the probability and what happened (0 is perfect).
  - **Two baselines**: "always comes" (p = 1, what a timetable implies), and the base rate (p = the
    share of trips that ran in the history window, the same for every trip). The model must beat
    both to be worth showing.
  - **Brier skill score** against each baseline: 1 − model / baseline (above 0 is better).
  - **Reliability**: predictions in ten bins of predicted probability; for each, the mean predicted
    value against the share that actually ran, with a 95% interval. A calibrated model sits on the
    diagonal. Plus the expected calibration error (the count-weighted mean gap).
- **Incremental and nightly.** Each night after `reconcile`, the job predicts yesterday and
  replaces that day's rows in `gold.calibration_predictions` (delete, then insert: idempotent).
  It then scores the last 28 days and writes `data/v1/calibration.json`, which the methodology
  page draws.
- **Which days count as history.** All fully collected trips, not only quality-passed days. The
  quality gate decides whether a *day's network rate* is fit to publish; a trip that was fully
  collected has a trustworthy outcome either way. This lets the backtest start on 16 November, not
  wait for the first day that passes the gate.

## Consequences

- The probability on the website is accompanied by evidence of how good it is, updated nightly.
- If the model is miscalibrated, the reliability diagram shows where (for example, too confident
  about `assigned` buses), which is the input for `heuristic-v2`.
- Trip start is used as the prediction point, matching the model's history cells (by trip start
  hour). Departures later in a route are not scored separately yet.
- Early days are thin: until about two weeks of history exist, many hours fall back to the route's
  overall rate, and some routes have no history at all (their predictions are `null` and are
  counted, not scored).
- Cost: one Glue Flex run a night replaying one day of the archive, about the same as `reconcile`.
