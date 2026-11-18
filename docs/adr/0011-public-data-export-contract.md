# ADR-0011: Public data export: a versioned JSON contract, quality-gated

**Status:** accepted, 18 November 2026. Roadmap 2.1. Code: `ghostbus_core.export` (pure),
`ghostbus_jobs.export` (Glue job `ghostbus-export`).

## Context

The website (roadmap 2.2) must keep working when every compute part is off, and it must never
show a number the quality gates rejected. The NTA licence (CC BY 4.0) requires attribution, a
link to the source and an "as is" statement on anything public, and the raw archive stays private:
only derived aggregates are published (`docs/02-nta-usage-policy.md`).

## Decision

- **Static JSON, written nightly** after the quality job, to a private S3 bucket under
  `data/v1/`. The website only ever reads these files; CloudFront will serve them in 2.2.

  | File | Content |
  |---|---|
  | `meta.json` | read first: generated time, which days are published, rules version, archive chain head, file list |
  | `days/{date}.json` | one service day: quality verdict, and rates with 95% intervals by operator, only if the day passed |
  | `league/7d.json`, `league/28d.json` | routes ranked worst first, published days only |
  | `routes/{slug}.json` | one route: totals, daily series, hour by day-type heatmap |
  | `stops.json` | plate code, name, position, routes serving it (current timetable) |

- **Quality-gated.** A day whose `gold.data_quality` overall is `fail` is listed, with its status
  and a plain-language note, but counted in no rate. A day without a quality result is not
  published either. A property test checks that no rate ever includes a failed day.
- **Conservative ranking.** Routes need at least 30 trips in the window, and are ordered by the
  *lower* end of the 95% Wilson interval of the untracked rate. "Worst" means confidently bad,
  not noisily bad. Smaller routes appear under `not_ranked` with their sample size.
- **A versioned contract.** Every document carries `schema: ghostbus.<kind>.v1` and validates
  against a JSON Schema in `ghostbus_core/schemas/export_*.v1.json`. A breaking change means a new
  `data/v2/`, never an in-place change, the same rule as the Kafka topics.
- **Deterministic bytes.** Keys are sorted and numbers rounded to 4 places, so the same data
  gives the same file. The job uploads only files whose MD5 differs from the stored ETag, so
  unchanged files keep their caches and cost no PUTs; only `meta.json` changes every night.
- **Attribution in every document**, not only on the website.

## Consequences

- The site's freshness is one night. Live departures ("will my bus come?", roadmap 2.3) come from
  the API, not from these files.
- A rule change (for example a new `RULES_VERSION`) reaches the public files on the next nightly
  run after `reconcile` re-decides history.
- Until a day passes the quality gates, the published rates are empty. That is the intended,
  honest state for the days collected while the laptop slept (15 to 17 November 2026).
