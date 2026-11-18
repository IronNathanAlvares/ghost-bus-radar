# ADR-0015: heuristic-v2: hierarchical empirical Bayes, and cancellations that aren't

**Status:** accepted, 19 November 2026. Follows ADR-0012 (the API) and ADR-0014 (the backtest).
Code: `ghostbus_core.model` (fit, probability, the published file), used by the API, the nightly
export (`data/v1/model.json`) and the backtest. Study: `docs/measurements/09-model-v2.md`.

## Context

The first backtest (measurement 08) showed `heuristic-v1` beating "the bus always comes" but not
a single base rate, and pointed at two causes: routes with little history got extreme guesses,
and every bus shown as cancelled got 0% although many still ran. Looking closer in the lake,
whether a cancelled bus still runs depends on the operator: Bus Éireann 41 of 70 cancellation
notices were followed by the bus running, Go-Ahead Ireland 2 of 13, Dublin Bus 0 of 11.

Research before designing:

- Rates from small samples are classically improved by **empirical-Bayes shrinkage** toward a
  group mean (Efron and Morris), with the strength of the pull estimated from how much the units
  really differ (a beta-binomial, fitted by the method of moments). The estimate is unstable with
  few units, so it is bounded.
- **Recalibration layers** (Platt scaling, isotonic regression) can fix a miscalibrated model after
  the fact; on small calibration sets isotonic regression overfits and Platt is safer
  (Niculescu-Mizil and Caruana). They hide *why* a model is wrong, so they come second.
- **Murphy's decomposition** of the Brier score (reliability − resolution + uncertainty) tells a
  calibration problem (reliability) from a discrimination problem (resolution).
- No published study measures how often GTFS-Realtime CANCELED trips still run; the NTA itself
  describes ghost buses as cancellations not entered in time. This has to be learned from our data.

## Decision

`heuristic-v2` keeps v1's core (the share of similar trips that ran, among the outcomes the live
status still allows) and changes the pooling:

1. **Three levels, shrunk in turn**: the hour's record toward its route's, the route's toward the
   network's. Prior strengths are estimated by the method of moments for each live-status case
   (scheduled, scheduled near departure, assigned, tracked) and bounded to [5, 2000] trips.
2. **Cancelled = the operator's record**: the share of that operator's pre-departure cancellation
   notices whose bus ran anyway, shrunk toward the network share with a prior of 10 notices.
3. **Routes without history get the network rate**, labelled as such, instead of no answer.
4. **No recalibration layer yet.** The structural fixes alone brought the calibration error
   (Murphy reliability) from 0.0077 to 0.0011 in the offline study; a Platt layer can be added if
   the nightly backtest shows a systematic tilt that stays.

The parameters the API needs beyond one route's file are published nightly as `data/v1/model.json`
(schema `ghostbus.model.v1`), fitted on the **published** days only, like every other number. The
backtest fits the same function on the 28 days before each test day, and stores v1's answer next
to v2's, so the methodology page compares them on exactly the same predictions.

**How v2 was chosen without fooling ourselves:** an offline harness on 56,388 stored backtest
predictions first reproduced every stored v1 probability exactly (0 differences), then scored the
candidates walk-forward. No parameter was tuned on the test days: the prior strengths are estimated
from each day's history, and the two fixed choices (the [5, 2000] bounds and the 10-notice prior)
were set before looking at results. The core module was then checked against the harness: 0
differences on all 56,388.

## Consequences

- In the offline study v2 beat v1 on every test day, lead time and live status, and beat the base
  rate overall (+11.8%) where v1 managed +4.4%.
- "Cancelled" is no longer shown as certain. The stop page says how often that operator's cancelled
  buses still ran.
- Still imperfect: for cancelled buses v2 says about 45% where 24% ran in the test days, because
  one early day of Bus Éireann reversals weighs heavily. More days will settle it; if not, the
  share could be made to depend on how long before departure the notice came.
- Until a day passes the quality gates, `model.json` has nothing to fit and the API answers as
  before ("no published history yet"); the backtest is unaffected, because it fits on all fully
  collected trips (ADR-0014).
