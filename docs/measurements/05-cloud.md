# Measurement 05: the cloud deployment

**Dates:** 17 to 18 November 2026. **Sprint:** S5. Design: ADR-0009 (cheap cloud), ADR-0010
(Glue, S3 Tables, Athena). Region `eu-north-1`.

## 1. Collection moved to AWS

- **Cutover:** the laptop poller stopped at 07:29:13 UTC on 18 November. Its last heartbeat
  (07:29:14) reached S3 before the schedule was enabled at 07:29:35.
- **First cloud minute:** skipped. EventBridge Scheduler's first delivery arrived at 07:30:14.8,
  past the planned call time, so the poller refused to call late.
- **Since then, every minute collected:** about 1,250 vehicle positions and 3,200 trip updates
  per call, the same as the laptop.
- **Invocation timing:** warm invocations start about 6 s into the minute and run for 9 to 14 s
  (mostly waiting for the planned second). They use at most 139 MB of the 256 MB configured, and
  a cold start adds 0.23 s.
- **Poll second:** moved from :15 to :25 for slack against late scheduler deliveries. The move
  exposed a pacing bug (calls stayed locked on their first second), now fixed: calls went
  07:42:15, then 07:43:25, then every minute at :25.
- **Coverage before the cutover:** the laptop made 471 calls on 16 November and 393 on 17 November,
  out of 1,440 a day; it collected only while awake.

## 2. Nightly batch in Glue

| Run | Wall time | DPU-seconds | Cost (Flex, $0.29/DPU-hour) |
|---|---|---|---|
| `timetable-load`, 4 versions from scratch | 254 s | 508 | $0.041 |
| `timetable-load`, incremental (2 more days of trip instances) | 55 s | 111 | $0.009 |
| `timetable-load --rebuild` (after the version-key fix) | 277 s | 555 | $0.045 |
| `reconcile`, 15 to 17 November | 215 to 258 s | 431 to 517 | $0.035 to $0.042 |

A normal night is one incremental load plus one day of reconcile: **about $0.04 a night, about
$1.20 a month**. The chain runs at 04:15 UTC from 18 November (`batch_enabled`).

Three bugs found by running for real, each fixed with a regression test (the third one proven to
fail on the old code):

1. **Glue executors could not read the driver's `/tmp`.** The first `timetable-load` read the
   unzipped CSVs as `file:/tmp/...`. On the laptop Spark is one machine; on Glue the executors are
   other machines. The CSVs are now staged in S3 (`STATIC_STAGE_URI`).
2. **Arguments starting with `--` were silently dropped.** The entry script's parser treated
   `--ARGS "--from 2026-11-15 ..."` as a flag, so the first backfill wrote only 17 November.
   Found by querying the result in Athena; the parser now reads known keys only.
3. **Two timetable zips shared one label.** NTA uploaded `14FEE109...` on 14 and 15 November with
   different calendars. Keyed by label, the second overwrote the first's per-version tables, and
   15 November had 7,003 trip instances instead of 12,185. The laptop never saw it because it had
   loaded only one version. Versions are now keyed by label plus SHA-256 prefix; after a
   `--rebuild`, 15 November has 12,185 instances, the same as the laptop.

`--rebuild` now empties the timetable tables (`DELETE FROM`) instead of dropping them. S3 Tables
refused `DROP TABLE ... PURGE` from Glue ("only supports dropping tables with purge enabled").

## 3. Athena over S3 Tables

Queries go through `"s3tablescatalog/ghostbus-lake"`. A `GROUP BY` over `gold.trip_outcomes`
scanned 2.8 KB and ran in 0.8 s. The workgroup caps every query at 1 GB scanned (at most half a
cent).

| Service date | Trips | Ran | Untracked | NO_DATA | In rates |
|---|---|---|---|---|---|
| Sun 15 Nov (from 19:31) | 12,185 | 2,631 | 177 | 8,258 | 1,804 |
| Mon 16 Nov | 19,762 | 6,784 | 51 | 10,474 | 589 |
| Tue 17 Nov (to 18:51) | 12,651 | 5,559 | 93 | 5,353 | 1,105 |

The `NO_DATA` counts are mostly the laptop asleep: it made 471 and 393 of 1,440 calls on 16 and
17 November. Collection moved to AWS at 07:29 on 18 November, so the first full cloud-collected day is
Thursday 19 November, decided by the nightly run on 20 November.

## 4. Nightly quality gates (roadmap 1.1 and 1.2)

A third Glue job, `ghostbus-quality`, runs when `reconcile` succeeds. It checks yesterday's
numbers (`ghostbus_core.quality`), upserts one row per check into `gold.data_quality`, and emails
a summary with OK, WARN or FAIL in the subject. Each check replays a real incident in
`tests/unit/test_quality.py`; one test caught that the first unmatched-id threshold (1%) would
have missed the 0.96% bug, so it fails at 0.5%.

First runs, 18 November (Glue Flex, 79 to 189 s, about 1 to 2 cents each):

| Day | Overall | Failing checks | Passing checks |
|---|---|---|---|
| Sun 15 Nov | FAIL | completeness 269 of 1,440 minutes; NO_DATA 72% of bus trips | scheduled 12,185 vs 12,184 a week away; unmatched 0 of 200,160 ids; decided 100% |
| Mon 16 Nov | FAIL | completeness 453 of 1,440; NO_DATA 60% | scheduled, unmatched 0 of 645,550, decided 100%, untracked 8.7% vs 8.5% |
| Tue 17 Nov | FAIL | completeness 359 of 1,440; decided 64%; NO_DATA 48% | scheduled, unmatched 0 of 566,071, untracked 8.4% vs 8.5% |

All three FAILs are true: those days were collected by the laptop, which slept. From 19
November (the first full day collected in AWS) the expected result is OK. Before the
version-key fix, 15 November's `scheduled_vs_week` would have read 7,003 vs 12,184 (0.57, FAIL);
before rules 5, its unmatched ids would have read 0.96% (FAIL).

Two bugs found on the first cloud run, both fixed with tests:
- **A Spark `Column` built at import time.** `quality.py` defined `F.col(...)` at module level,
  which needs a running SparkContext; Glue imports the module before Spark starts. Every test
  passed because they start Spark first. A new unit test imports every Glue task module in a
  fresh interpreter without Spark.
- **The email subscription was never confirmed.** SNS accepted the messages and delivered none
  (`PendingConfirmation`). Confirming the subscription email is a one-time owner step.
