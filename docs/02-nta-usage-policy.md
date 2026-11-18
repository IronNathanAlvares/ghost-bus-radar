# NTA developer usage policy: what it allows

**Read:** 15 November 2026, at <https://developer.nationaltransport.ie/usagepolicy>. Paraphrased
here; the policy itself is authoritative and NTA may change it at any time, so re-read it before
anything goes public (Sprint 6).

## What it says, and what the project does about it

| Topic | Policy (paraphrased) | What Ghost Bus Radar does |
|---|---|---|
| Call rate | Each access token may call the GTFS Real Time API once every 60 seconds. Usage limits are per day and do not carry over. NTA may change a user's limit | One call per minute across all three endpoints, feeds take turns (ADR-0005). A restarted poller reads its last call time from the archive so it never calls early. No immediate retries |
| Excessive load | Automated scripts that place excessive demand on the infrastructure are prohibited | One poller per key, anywhere. The static zip is checked with a HEAD request hourly and downloaded only when it changes |
| Tokens | Each user gets a pair of tokens. They are non-transferable and all use is attributed to the user | Key only in `.env` (git-ignored), never logged, never in the repo. The second token is **not** used to double the call rate |
| Licence | Data is licensed CC BY 4.0 | Raw archive kept for reproducibility; derived numbers published |
| Attribution | Public-facing apps, presentations and publications must credit the NTA, link to the source, and state that the data is provided as is, without NTA responsibility for errors | Attribution block on every website page, in the README, the dashboard and the write-up |
| Commercial use | Using the data mainly for profit without adding significant value is prohibited | Non-commercial portfolio and research project that adds analysis |
| Integrity | Data must not be altered in a misleading way; apps should stay up to date | Ghost categories with stated limits ("untracked", not "never ran"), sample sizes and confidence intervals; methodology page |
| Privacy | No use that breaches privacy law (GDPR) | No personal data is collected. Vehicle ids are fleet numbers, not people |
| Liability | Data is provided as is; users indemnify NTA | Stated on the methodology page |

## Open questions

- **Is the 60 s limit per endpoint or for the whole API?** The policy says "the GTFS Real Time
  API", so we assume the whole API. If NTA confirms it is per endpoint, the poller can call
  TripUpdates and Vehicles every minute each by changing `feeds.feed_for_slot` only.
- **Higher limit for research use.** The policy lets NTA change a user's limit. If two-minute
  resolution turns out to limit the ghost classification (Sprint 4), ask NTA at the contact
  address in `feed_info.txt`.
- Storage and redistribution of the raw realtime archive are not mentioned. The archive stays
  private; only derived aggregates are published.
