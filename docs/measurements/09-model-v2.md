# Measurement 09: heuristic-v2, and how it was chosen

**Date:** 19 November 2026. **Design:** ADR-0015. **Backtest:** ADR-0014, three test days
(16, 17, 18 November); 18 November is the first day collected completely by the cloud and carries
most of the predictions.

## 1. More data first

- Reconciled 18 November straight away (Glue, 318 s) instead of waiting for the nightly run.
- Re-ran the backtest for 16 to 18 November with v1 (342 s): **56,388 predictions**, up from 6,776.
- Pulled two tables from the lake for an offline study (kept out of the repository): outcome counts
  per day, route and hour (4,614 rows, with cancellation notices), and every stored prediction.

## 2. What the data showed about cancellations

Bus trips with a cancellation notice before their scheduled start, and how many ran anyway
(15 to 17 November, fully collected trips):

| Operator | Notices | Ran anyway |
|---|---|---|
| Bus Éireann | 70 | **41 (59%)** |
| Go-Ahead Ireland | 13 | 2 (15%) |
| Dublin Bus | 11 | 0 (0%) |

Bus Éireann's notices came about 75 minutes before departure, on average, both for the buses that
then ran and those that didn't.

## 3. The offline study

**Faithfulness first.** The harness recomputed v1 through the production code for every stored
prediction: **0 differences in 56,388**. Only then were candidates compared, walk-forward (each
test day's model fitted on the days before it), with nothing tuned on the test days.

Scored on the same 54,834 predictions (those v1 could answer):

| Candidate | Brier | vs "always comes" | vs base rate | Reliability | Resolution |
|---|---|---|---|---|---|
| v1 | 0.0973 | +15.1% | +4.4% | 0.0077 | 0.0128 |
| v2a: hour → route → network shrinkage | 0.0904 | +21.1% | +11.2% | 0.0015 | 0.0126 |
| v2b: + cancelled by operator | **0.0898** | **+21.7%** | **+11.8%** | **0.0011** | 0.0128 |
| v2c: + network rate for routes without history | 0.0898 | +21.7% | +11.8% | 0.0011 | 0.0128 |

v2c answers 1,554 more predictions (routes with no history yet) at no cost to the rest, so v2 = v2c.
Murphy's decomposition says where the gain comes from: **reliability (the calibration penalty) fell
to a seventh**, while resolution (telling buses apart) held.

v2 against v1 in every slice (Brier skill against the base rate):

| Slice | v1 | v2 |
|---|---|---|
| 16 Nov (1 day of history) | −18.0% | +0.2% |
| 17 Nov (2 days) | −3.7% | +5.0% |
| 18 Nov (3 days) | +6.2% | +13.1% |
| 30 min before | −0.6% | +8.4% |
| 15 min before | +1.7% | +10.1% |
| 5 min before | +6.7% | +13.3% |
| 1 min before | +9.6% | +15.4% |
| status scheduled | −0.9% | +5.5% |
| status assigned | −8.6% | +17.3% |
| status tracked | +9.9% | +23.9% |
| status cancelled | +59.0% | +66.3% |

Prior strengths the method of moments estimated (route, hour), in trips: for buses with nothing
live yet, from (5, 2000) with one day of history to (6, 74) with three; for tracked buses
(22, 2000) to (18, 23). In plain words: routes really do differ, so each route's own record counts
quickly, and with more days hours start to differ within a route too.

The core module (`ghostbus_core.model`) was then checked against the harness: **0 differences in
56,388**.

## 4. In the cloud

The deployed backtest (Glue, 304 s) reproduced the study exactly and now publishes the comparison:

| | v1 | v2 |
|---|---|---|
| Brier, same 54,834 predictions | 0.0973 | **0.0898** |
| Calibration error | 6.3% | **2.1%** |
| Beats "always comes" | yes (+15.1%) | **yes (+21.7%)** |
| Beats one base rate | yes, narrowly (+4.4%) | **yes (+11.8%)** |

Over all 56,388 predictions (v2 answers every one): Brier 0.0943, +21.5% against "always comes",
+10.9% against the base rate, calibration error 1.9%.

## 5. Still wrong, and why

| Predicted | Predictions | Share that ran | What it is |
|---|---|---|---|
| 20–30% | 324 | 3% | cancelled buses of operators that rarely reverse a cancellation |
| 40–50% | 186 | 19% | cancelled Bus Éireann buses |

For cancelled buses v2 says 45% on average where 24% ran. The operator shares were learned mostly
from 15 November, when Bus Éireann reversed an unusual number of cancellations. More days will
show whether that was one bad day. If it persists, the next step is to make the share depend on
how long before departure the notice came.

The live API already uses v2. Until a day passes the quality gates it still says "no published
history yet", because `model.json` is fitted on published days only.
