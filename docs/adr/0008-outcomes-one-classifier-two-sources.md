# ADR-0008: Trip outcomes: one classifier, two sources, provisional then final

**Status:** accepted, 15 November 2026. Rules: `docs/03-ghost-definition.md`.

## Context

The brief asks for a backfill path for outages, and the plan promised that live and backfill
would share one code path "enforced, not promised". Outcomes also have to be honest about our
own downtime, and decisions about a trip are only possible once the bus could have finished.

## Decision

- **One pure classifier** (`ghostbus_core.outcomes.classify`) over an `Evidence` record. Spark
  calls it through a UDF; tests, the sensitivity analysis and the field check call it directly.
  Rules carry a version (`RULES_VERSION`) stored with every outcome with the thresholds.
- **One evidence builder** (`ghostbus_jobs.outcomes`) over silver-shaped DataFrames, with two
  sources of those frames:
  - the streaming silver tables (live);
  - the raw archive, normalised by the same `ghostbus_core.gtfsr` code and passed through the same
    `stream.*_frame` functions the streaming queries use (batch dedupe instead of watermark dedupe,
    identical in result when all data is present). Executors read the archive directly, so a day of
    snapshots never passes through the driver.
- **Realtime to schedule** goes through the version live at fetch time and the natural trip key
  (ADR-0006), so it survives a timetable change mid-day.
- **Evidence is bounded** to data fetched by each trip's close time, so it does not depend on
  when or from which source it is computed.
- **Coverage** comes from heartbeats of the same source, as gap intervals computed in the driver
  (`ghostbus_core.coverage`, pure and property-tested) and range-joined to trip windows.
- **Provisional, then final.** `trip_state` (a `foreachBatch` stream on silver heartbeats) decides
  trips a few minutes after they close, using the *data* clock (newest heartbeat minus the
  watermark), so replays recompute identical rows. `reconcile` decides from the archive and writes
  `final`. Both `MERGE` on the trip-instance key; final is never overwritten by provisional, and
  identical rows are left untouched.
- **Backfill is reconcile over a date range.** The collector writes the archive whether or not
  Kafka or the stream is running (ADR-0001), so any stream outage is repaired by it.

## Proof

`backfill_demo` computes outcomes three ways over the same closed trips: from the uninterrupted
stream, from the stream with an outage cut out, and from the archive alone. Backfill must equal
uninterrupted row for row, and the outage must produce `NO_DATA`, never a ghost
(`docs/measurements/04-outcomes.md`). Integration tests cover every outcome on a synthetic week
that spans a timetable version change.

## Consequences

- Published rates use fully collected trips only (`in_rates`), since a gap removes bad outcomes
  but not good ones.
- Corrections (deleting bad rows) are applied to every layer; streaming readers skip delete and
  overwrite snapshots instead of stopping. `lake_audit` checks the lake against the archive.

- A rule change is a code change with a version bump; rerunning `reconcile` re-decides history.
- Provisional outcomes can differ from final ones where the stream's watermark dropped late rows.
  The difference is measurable and the final outcome is the one published.
