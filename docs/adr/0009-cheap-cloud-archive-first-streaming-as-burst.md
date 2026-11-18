# ADR-0009: Cheap cloud: always-on collection and batch, streaming only in bursts

**Status:** accepted, 16 November 2026. Supersedes the "showcase for 2 to 4 weeks" default in
PLAN section 9.

## Context

The owner wants the AWS bill as small as possible. PLAN section 9 priced a showcase with MSK and
an always-on EMR Serverless streaming job at $200 to $260 a month while on. Sprint 4 showed that
the parts that matter for the published numbers do not need either:

- Final outcomes come from the raw archive (`reconcile`), and backfill equals the stream on every
  column (measurement 04).
- The archive is written by the poller whether or not Kafka exists (ADR-0001).

What streaming adds is minutes-fresh provisional outcomes and a cloud latency number. Both can be
shown for a few hours instead of paid for every hour of the month.

## Decision

Three Terraform layers, each with its own state:

| Layer | What | Runs | Expected cost |
|---|---|---|---|
| `bootstrap` | State bucket, monthly budget alert | Always | Free |
| `core` | Raw archive (S3), poller and static watcher Lambdas, alarms; later the lake (S3 Tables, Athena) and the nightly batch (EMR Serverless, billed per job) | Always | About $1 to $3 a month |
| `stream` (later in S5) | VPC, MSK, EMR Serverless streaming job | A few hours at a time, then `terraform destroy` | A few dollars per burst |

- **The Lambdas sit outside any VPC.** NTA and S3 are reached directly, so there is no NAT gateway
  ($35 a month) and no fck-nat instance.
- **The poller is stateless.** EventBridge Scheduler fires it at the start of every minute.
  - It reads the newest heartbeats in the archive to learn the last call and any backoff, then
    plans exactly as the local poller does (`pacing.planned_call_at`). So the 60 s rule holds
    across invocations and across the move from the laptop.
  - Scheduler delivers at least once. Each invocation therefore claims its minute with an S3
    conditional write (`If-None-Match: *` on `claims/minute=<n>`). Exactly one invocation can win
    a minute; the claims expire after a day.
  - The usual alternative, reserved concurrency of 1, is often unavailable on new accounts
    (unreserved concurrency must stay at 10 or more).
- **Collect mode publishes nothing to Kafka.** The Lambda package leaves out `confluent-kafka`
  (6.3 MB zip, arm64). A burst replays the archive through MSK instead, which also exercises the
  replay path.
- **The NTA key lives in SSM Parameter Store** (SecureString, standard tier: free), written by
  `infra/scripts/put_nta_key.py`. Terraform knows only the parameter name, so the key is never in
  state.
- **Cost guards in code:**
  - The budget alert at 25%, 50% and 100% of $20, plus a forecast alert.
  - Log retention of 14 days.
  - Later: EMR Serverless maximum capacity, auto-stop, and an Athena per-query scan limit.

## Account constraints (found on deploy, 17 to 18 November 2026)

The account came from AWS's new sign-up experience on the Free plan. AWS manages its
organization and SCPs: one region (`eu-north-1`, from the contact address), no EMR, no MSK, no
Kinesis (the Paid plan adds EMR and MSK), and no IAM Identity Center. The collector design above
needs none of these. For the nightly batch, the choice is AWS Glue (Spark 3.5 with Iceberg,
allowed on the Free plan) or EMR Serverless after an upgrade to the Paid plan with a spend limit.
The Free plan cannot bill, but it closes the account after six months unless upgraded.

## Consequences

- In the cloud, outcomes are final and daily, not live. The website's daily JSON export (already
  planned for the parked mode) becomes its normal mode. Live departures are a burst-only demo
  unless the owner later chooses to pay for streaming.
- The cloud latency claim is measured during a burst and stated with its date. It is not a
  continuous SLO.
- The laptop stack stays the development and streaming environment. The archive moves to S3 and
  can be synced back for local work.
- Moving the poller is a cutover with a few minutes of `NO_DATA`. Stop the laptop poller, sync
  the archive up, then enable the schedule (`docs/05-aws-setup.md`).
