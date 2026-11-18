# Measurement 08: is "will my bus come?" calibrated?

**Date:** 18 November 2026. **Roadmap:** 2.4. Design: ADR-0014. Glue job `ghostbus-calibration`
(Flex, 2 × G.1X), run once for 16 and 17 November: 271 s.

## 1. What was scored

Every fully collected bus trip on 16 and 17 November, asked at 30, 15, 5 and 1 minutes before its
scheduled start, with the live status replayed from the archive and history from the days before
only (walk-forward). 6,776 predictions; 6,301 scored; 475 had no history for their route yet
(counted, not scored). 86.2% of the scored trips ran.

## 2. Scores

Brier score, lower is better (0 is perfect).

| | Model `heuristic-v1` | "Always comes" | One base rate | Skill vs always comes | Skill vs base rate |
|---|---|---|---|---|---|
| **All** | **0.128** | 0.138 | 0.119 | **+7%** | **−8%** |
| 30 min before | 0.137 | 0.138 | 0.119 | +0% | −15% |
| 15 min before | 0.134 | 0.138 | 0.120 | +3% | −12% |
| 5 min before | 0.122 | 0.138 | 0.119 | +11% | −2% |
| 1 min before | 0.120 | 0.138 | 0.119 | +13% | −1% |

**Verdict:** the roadmap's bar, beating "the bus always comes", is met, and the edge grows as
departure nears and live evidence arrives. The stricter bar set in ADR-0014, beating one base rate
for every bus, is **not** met yet. Calibration error 10.4%.

## 3. Where it goes wrong (the reliability diagram)

| Predicted | Predictions | Average predicted | Share that ran | 95% interval |
|---|---|---|---|---|
| 0–10% | 222 | 0.0% | 36.0% | 30.0–42.5% |
| 10–20% | 20 | 17.0% | 40.0% | 21.9–61.3% |
| 20–30% | 29 | 25.0% | 93.1% | 78.0–98.1% |
| 30–40% | 32 | 31.9% | 87.5% | 71.9–95.0% |
| 40–50% | 116 | 42.4% | 81.0% | 73.0–87.1% |
| 50–60% | 154 | 52.7% | 82.5% | 75.7–87.7% |
| 60–70% | 347 | 64.8% | 83.9% | 79.6–87.4% |
| 70–80% | 379 | 76.1% | 80.7% | 76.5–84.4% |
| 80–90% | 749 | 84.9% | 81.6% | 78.6–84.2% |
| 90–100% | 4,253 | 98.6% | 90.8% | 89.9–91.6% |

By live status at the time:

| Status | Scored | Average predicted | Share that ran | Brier |
|---|---|---|---|---|
| scheduled | 5,265 | 88.0% | 86.2% | 0.141 |
| assigned | 471 | 94.9% | 97.0% | 0.044 |
| tracked | 428 | 97.0% | 96.0% | 0.043 |
| cancelled | 137 | 0.0% | 20.4% | 0.204 |

Three findings:

1. **"Cancelled" is not final.** 28 of 137 trips that showed as cancelled at prediction time then
   ran (tracked through 60% or more of their stops: the classifier's "ran despite a cancellation
   notice"). The model says 0% for every cancelled bus; on this evidence the honest figure is about
   20%. A real-world fact about the feed, found only by replaying it.
2. **Thin history makes extreme guesses.** With one or two days of history, many routes have only
   a handful of trips, so their own rate is 100% (top bin: said 98.6%, 90.8% ran) or dragged down by
   one or two ghosts (the middle bins: said 25% to 76%, 81% to 93% ran). The model shrinks an hour
   toward its route, but not the route toward the network.
3. **Live evidence works.** Where the status rules outcomes out (assigned, tracked), predictions are
   close to reality (Brier 0.04). The weakness is buses with nothing live yet.

## 4. What it implies (heuristic-v2, not built yet)

- Shrink each route's rate toward the network rate (an empirical-Bayes prior worth some tens of
  trips) before shrinking the hour toward the route: fixes finding 2.
- Give "cancelled" the measured chance that a cancelled bus still runs, learned from the backtest's
  own history, instead of 0: fixes finding 1, and is worth saying on the stop page ("cancelled, but
  1 in 5 of these still ran").
- Re-score with the same backtest; ship v2 only if it beats both baselines, with more days of history.

Two days is a small sample, and the history behind them is one or two days long; the nightly run
adds a day at a time, so these numbers will move.
