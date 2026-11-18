# Diagrams: how Ghost Bus Radar works

Flowcharts and sequence diagrams for presenting the project and preparing for technical
interviews. They are written in [Mermaid](https://mermaid.js.org/), which GitHub renders directly,
so they live next to the code and change with it. Every diagram has a one-sentence summary, a
walkthrough, the questions an interviewer is likely to ask (with answers), and beginner notes.

| File | Diagrams |
|---|---|
| [1. The big picture](01-big-picture.md) | what it does; where it runs (AWS vs laptop); data layers raw to gold |
| [2. Collection](02-collection.md) | one minute of the cloud poller; pacing rules; feed schedule; static watcher; cutover |
| [3. The timetable](03-timetable.md) | loading a version (SCD2); matching live trips to scheduled ones; GTFS time; tables |
| [4. Streaming](04-streaming.md) | Kafka to bronze to silver; exactly once under SIGKILL; watermarks and late data |
| [5. Outcomes](05-outcomes.md) | the classifier; gaps become NO_DATA; provisional vs final; which trips count in rates; nightly quality gates |
| [6. The cloud](06-cloud.md) | AWS architecture; IAM and guardrails; Terraform layers; costs |
| [7. Engineering](07-engineering.md) | test pyramid; CI; secrets; the bugs measurements found |

## A 10-minute presentation

| Minute | Show | Say |
|---|---|---|
| 0-1 | 1.1 | The question: does the bus on the board actually come? Two NTA inputs, one honest answer per trip. |
| 1-2 | 1.3 | Raw archive first; every layer can be rebuilt from it. |
| 2-4 | 2.1, 2.2 | One API call a minute: a stateless Lambda, a lock in S3, never a retry inside the minute. |
| 4-5 | 3.2 | Trip ids change every night, so live trips are matched by natural key. |
| 5-7 | 5.1, 5.4 | The rules, why our own downtime can never create a ghost, and the survivorship bias we fixed. |
| 7-8 | 5.3 | Live and backfill share one code path; proved equal on every column of 10,528 trips. |
| 8-9 | 6.1, 6.4 | The AWS design and why it costs a few dollars, not $200 a month. |
| 9-10 | 7.4 | One bug story told with STAR; what's next. |

## Question bank (short answers; details in each file)

**Design**
- *Why archive raw data before Kafka?* Kafka keeps 7 days; the archive keeps everything, makes
  backfill possible after any outage, and costs cents (1.3, ADR-0001).
- *What is your source of truth?* The raw S3 archive. Everything else is derived and rebuildable.
- *Batch or streaming?* Both, with one shared code path. Streaming gives provisional answers in
  minutes; the nightly batch gives final answers from the archive (5.3).
- *How do you handle schema or id changes upstream?* Natural keys plus a timetable history (3.2);
  Kafka topics carry a major version (`.v1`).

**Reliability**
- *What if the poller runs twice?* An S3 conditional write lets exactly one invocation own each
  minute (2.1).
- *What if Spark crashes mid-write?* Checkpointed offsets plus Iceberg's idempotent commits plus
  dedupe; proved with 20 SIGKILLs and a row-for-row comparison (4.2).
- *What if collection stops for an hour?* Affected trips become `NO_DATA`, never ghosts, and
  are excluded from rates; the rest is repaired from the archive (5.2, 5.3).
- *How are you alerted?* No snapshot for 10 minutes, a failing static watcher, or a failed Glue job
  emails an SNS topic (6.1).

**Data quality and statistics**
- *How do you know a number is right?* Field check against people at bus stops (pending),
  comparisons between independent paths, and Wilson intervals with sample sizes on every rate.
- *What bias did you find?* Survivorship through gaps: 94.4% ran became 87.2% (5.4).

**Cloud and cost**
- *Why Lambda, Glue, S3 Tables and Athena?* Pay-per-use for a workload that runs one minute a
  minute and one job a night; nothing idles (6.1, 6.4, ADR-0009, ADR-0010).
- *How do you manage infrastructure?* Terraform in two layers with remote state and locking;
  features off by default; plans reviewed before every apply (6.3).
- *How are secrets handled?* SSM SecureString, read by one role; never in code, logs, git or
  Terraform state (7.3).

## Glossary

| Term | Meaning |
|---|---|
| GTFS / GTFS-Realtime | Standard formats for timetables (CSV in a zip) and live updates (protobuf) |
| Trip instance | One scheduled run of a trip on one service date |
| SCD2 | Slowly changing dimension type 2: keep every past version of a row with validity dates |
| Medallion (bronze, silver, gold) | Layers from raw to cleaned to business answers |
| Iceberg | Open table format: database-style commits and history over Parquet files |
| Watermark | How long a stream waits for late events |
| Exactly once | No row lost and none duplicated, even across crashes and retries |
| Idempotent | Repeating an operation changes nothing |
| Lambda / Glue / Athena | Serverless function / serverless Spark / serverless SQL |
| S3 Tables | S3 buckets built for Iceberg tables, with automatic maintenance |
| IAM role / SCP | Permissions a service assumes / organization-wide limits above them |
| Terraform state | Terraform's record of what exists, compared with the code on every plan |
| Wilson interval | A confidence interval for a proportion that behaves well near 0% and 100% |
