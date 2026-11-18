# ADR-0006: How the timetable is kept as history

**Status:** accepted, 15 November 2026. Refines ADR-0002.

## Context

NTA republishes the static timetable as often as daily and regenerates every `route_id` and
`trip_id` each time. Realtime ids only match the version they were published against. Ghost
outcomes for a past day must be computed against the timetable that was in force that day, and a
route's history must survive renames. Profiling the real feed (Sprint 2) found three things the
original keys did not handle:

- Irish Rail names every one of its 19 routes `rail`, so `(agency, short name, type)` merged
  them into a single route.
- On a weekday about 30 pairs of trips (almost all Luas) share route, direction, first
  departure and first stop; they differ only in `block_id`.
- The current timetable crosses a clock change (28 March 2027).

## Decision

**Keys**
- Route = `agency_id|route_short_name|route_type`, plus `|route_long_name` for rail
  (`route_type` 2). A rule by type, not "add the long name when there is a clash", so a route's
  key cannot change because some other route appeared.
- Trip instance = route key, direction, service date, first departure (`HH:MM:SS`, GTFS time),
  first stop, and `|n` for the n-th duplicate. Duplicates are ordered by last stop, stop count,
  last arrival, then `trip_id`, so identical twins get stable indices.

**Versions**
- A version is valid from its publish time (`Last-Modified` of the zip), until the next one.
- Realtime ids are matched against the version **live at fetch time**.
- A service day's schedule comes from the version **live at 04:00 local that day**. A version
  published at 23:09 (as the current one was) takes over the next day; a mid-day upload never
  rewrites a day under way. The natural key bridges the rare case where the two rules disagree.

**Tables** (`lake.dim`, Iceberg format v2)

| Table | Kind | Key |
|---|---|---|
| `feed_version` | One row per version, rewritten whole; the commit marker | `feed_version` |
| `route`, `stop` | SCD type 2: `row_hash`, `valid_from`, `valid_to`, `is_current` | natural key |
| `route_version`, `trip`, `stop_time`, `service_date` | Per version, partitioned by `feed_version`, raw NTA ids | id within version |
| `trip_instance` | One row per scheduled trip per service day, partitioned by `service_date`, UTC start and end | `trip_instance_key` |

**Times** follow the GTFS reference exactly: seconds after local noon minus 12 hours. On
the spring clock change that origin is 23:00 the evening before; daytime times always equal
the wall clock. Computed once per day in Python (`timetable.service_day_origin`), added in Spark.

**Writes are idempotent.** SCD2 is one `MERGE` per dimension (a changed row is closed and its
replacement inserted in the same commit; a withdrawn key is closed by `NOT MATCHED BY SOURCE`).
Per-version tables and trip instances use partition overwrite. `feed_version` is written last.
A failed run is repaired by running again; the job retries itself up to three times.
A version older than the loaded history is refused unless `--rebuild` replays all of them.

## Consequences

- A trip from any past day joins to the route and stop rows valid at its scheduled start
  (tested: renamed route, withdrawn route, renamed stop, all ids regenerated).
- Small dimensions (400 routes, 10,000 stops) are built in the driver with the same functions
  as the pure reference implementation, and an integration test asserts the Spark `MERGE` result
  equals `timetable.scd2_apply`.
- `stop_time` stores 7.4 M rows per version (25 MB of Parquet). Daily uploads would add about
  9 GB a year; acceptable, and deduplicating unchanged versions is possible later if needed.
- Service days before the first archived version have no schedule (14 November 2026 here).
