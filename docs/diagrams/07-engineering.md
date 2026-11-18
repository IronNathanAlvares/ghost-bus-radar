# 7. Engineering practice: tests, CI, secrets, and how bugs were found

Interviewers ask "how do you know it works?" more than "what does it do?". These diagrams answer
that.

## 7.1 The test pyramid

**Say it in one breath:** fast pure tests and property tests at the bottom, real Kafka, Spark and
S3 in Docker in the middle, and a chaos test that kills the pipeline at the top.

```mermaid
flowchart BT
    U["unit + property tests (156)<br/>pure functions: pacing, schedule, classifier,<br/>SCD2, coverage, Lambda poller with a fake clock<br/>~10 s, no Docker"]
    I["integration tests (33)<br/>real Kafka, Spark, Iceberg REST, SeaweedFS:<br/>every outcome on a synthetic week across a<br/>timetable change; stream dedupe; late data<br/>~2 min"]
    C["chaos test<br/>SIGKILL the pipeline 10-20 times,<br/>compare row for row with a clean run<br/>~5-8 min"]
    M["measurements on real data<br/>backfill = stream; outage -> no ghosts;<br/>0.00% unmatched trip ids"]
    U --> I --> C --> M
```

**Interviewer asks**
- *Why is most logic in pure functions?* Pure functions (no clock, no network, no Spark) can be
  tested exhaustively and quickly. The Lambda and the laptop poller share the same pacing function,
  and the stream and the batch share the same classifier.
- *Give an example of a property test.* "For any evidence, a collection gap never produces a
  ghost." Hypothesis tries hundreds of random cases; it found a real rule bug in Sprint 4.

## 7.2 Continuous integration

```mermaid
flowchart LR
    PUSH(["push or pull request"]) --> A["checks:<br/>ruff format + lint,<br/>mypy strict,<br/>pytest (unit)"]
    PUSH --> T["terraform:<br/>build Lambda zip,<br/>fmt -check, validate<br/>(no AWS credentials)"]
    A --> B["integration:<br/>build Spark image,<br/>docker compose up,<br/>integration tests,<br/>chaos test (10 kills),<br/>latency probe"]
```

- CI never touches the AWS account or the NTA API: no secrets in CI, and no live calls in any test.

## 7.3 How secrets flow

**Say it in one breath:** the NTA key exists in exactly two places, `.env` on the laptop
(git-ignored) and SSM in AWS, and nothing in between ever prints or stores it.

```mermaid
flowchart LR
    ENV[".env on the laptop<br/>(git- and docker-ignored)"] -->|"put_nta_key.py:<br/>read, send, never print"| SSM["SSM SecureString<br/>/ghostbus/nta-api-key"]
    SSM -->|"GetParameter at cold start<br/>(poller role only)"| L["Lambda memory"]
    ENV -->|"env_file"| DOCKER["laptop containers"]
    TF["Terraform state"] -.->|"knows the NAME only"| SSM
    CREDS["~/.aws/credentials<br/>(nathan-admin key)"] -->|"profile ghostbus"| CLI["AWS CLI, Terraform, scripts"]
```

## 7.4 Bugs the measurements found (good interview stories)

```mermaid
flowchart TB
    subgraph S2["Sprint 1-3"]
        direction TB
        B1["restarted poller could call within 60 s<br/>-> read last call from heartbeats"]
        B2["feed schedule left a 4-min gap<br/>-> property test, count non-alert slots"]
        B3["Iceberg streaming source failed on file:// checkpoints<br/>-> ResolvingFileIO"]
        B1 ~~~ B2 ~~~ B3
    end
    subgraph S4["Sprint 4"]
        direction TB
        B4["survivorship: 94% ran was really 87%<br/>-> rates over fully collected trips"]
        B5["test fixture leaked into live tables<br/>-> isolated topics + lake_audit"]
        B6["a DELETE would crash the streams<br/>-> skip correction snapshots"]
        B7["evidence depended on 'now'<br/>-> bound to the trip's close time"]
        B4 ~~~ B5
        B6 ~~~ B7
    end
    subgraph S5["Sprint 5"]
        direction TB
        B8["calls locked on their first second<br/>-> realign rule, verified live"]
        B9["Glue executors could not read the driver's /tmp<br/>-> stage CSVs in S3"]
        B10["account guardrails denied eu-west-1<br/>-> policy simulator, move to eu-north-1"]
        B11["Glue silently dropped arguments starting with --<br/>(only 1 of 3 days backfilled)<br/>-> read known keys; checked with Athena"]
        B12["two timetable zips with the same NTA label:<br/>the second overwrote the first,<br/>15 Nov lost 5,182 of 12,185 trips<br/>-> version key = label + SHA prefix"]
        B13["realtime ids lagged a timetable publish by 76 min:<br/>24 false ghosts, 92 false 'vanished'<br/>-> previous-version fallback, rules 5"]
        B8 ~~~ B9
        B14["quality job built a Spark Column at import:<br/>passed every test, crashed in Glue<br/>-> lazy Column + no-Spark import test"]
        B10 ~~~ B11 ~~~ B12 ~~~ B13 ~~~ B14
    end
    S2 ~~~ S4 ~~~ S5
```

**How to tell one of these stories (STAR):** the **situation** (e.g. "the first rate said 94% of
buses ran"), the **task** ("is that number honest?"), the **action** ("split by coverage; found
1,040 'ran' decisions made through gaps where a missing bus would have been NO_DATA; changed the
rate sample and added a property test"), and the **result** ("87.2%, with the reason documented").
