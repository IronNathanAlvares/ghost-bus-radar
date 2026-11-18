# ADR-0005: One API call per minute, shared by all feeds

**Status:** accepted, 15 November 2026

## Context

The plan assumed TripUpdates and Vehicles could each be polled every 60 s, from forum reports.
The NTA usage policy (`docs/02-nta-usage-policy.md`) says a token may call the GTFS Real Time API
once every 60 seconds. It does not say per endpoint. Sprint 1 also planned to "measure the real
rate limit" by backing off on 429s, but probing above a published limit would break the policy.

## Decision

- **One call per UTC minute, for the whole API.** The minute decides the feed
  (`ghostbus_core.feeds.feed_for_slot`): one minute in fifteen is Alerts; the rest alternate
  strictly between TripUpdates and Vehicles. Each of the two main feeds is refreshed every two
  minutes and never waits more than three.
- **Never closer than 60 s.** `ghostbus_core.pacing.next_call_at` enforces it even when a call is
  slow, and a restarted poller reads its last call from the newest heartbeat in the archive.
- **No immediate retries.** A retry is a call. A failure costs one snapshot; repeated failures
  skip 1, 3, 7 then 15 minutes, and a 429 honours `Retry-After`.
- **The rate limit is not probed.** We record any 429s we receive at this rate instead.
- **The schedule is a pure function of the clock**, so the AWS Lambda (fired every minute, no
  state) and the local poller choose the same feed for the same minute.
- **One poller per key.** Moving collection to AWS means stopping the local poller first.

## Consequences

- Each feed has two-minute resolution, not one. That is enough for the outcomes in PLAN section 6
  (cancellation timing has a 15-minute threshold, VANISHED needs a 15-minute loss of tracking,
  RAN needs 60% of stops), but a short-lived prediction can now be missed. Sprint 4 measures
  whether this matters; if it does, ask NTA for a research limit.
- Collection is cheaper than planned: 1,440 calls a day instead of 2,880.
- If NTA confirms the limit is per endpoint, only `feed_for_slot` changes.
