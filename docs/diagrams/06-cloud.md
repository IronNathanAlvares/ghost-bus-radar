# 6. The cloud: AWS services, permissions, deployment and cost

What runs in AWS, who is allowed to do what, how it is deployed with Terraform, and how the bill
is kept to a few dollars.

## 6.1 AWS architecture (eu-north-1)

**Say it in one breath:** two schedules drive two Lambdas that fill an S3 archive; a nightly
Glue chain turns the archive into Iceberg tables in S3 Tables; Athena queries them through the
Glue Data Catalog; CloudWatch, EventBridge and SNS email when something breaks.

```mermaid
flowchart TB
    subgraph COLLECT["collect (always on)"]
        S1["Scheduler<br/>cron every minute"] --> L1["Lambda ghostbus-poller<br/>arm64, 256 MB"]
        S2["Scheduler<br/>hourly at :23"] --> L2["Lambda ghostbus-static-watcher<br/>512 MB, 1 GB /tmp"]
        SSM["SSM SecureString<br/>NTA key"] -.-> L1
    end
    NTA["NTA APIs"] <--> L1
    NTA <--> L2
    L1 --> ARC[("S3 ghostbus-archive<br/>versioned, delete-protected")]
    L2 --> ARC

    subgraph BATCH["nightly batch"]
        TR["Glue trigger 04:15 UTC"] --> J1["Glue job timetable-load"]
        J1 -->|"on SUCCEEDED"| J2["Glue job reconcile"]
        J2 -->|"on SUCCEEDED"| J3["Glue job quality<br/>checks into gold.data_quality<br/>+ daily email"]
        J3 -->|"on SUCCEEDED"| J4["Glue job export<br/>public JSON"]
    end
    J4 --> SITE[("S3 ghostbus-site<br/>data/v1")]
    ARC --> J1
    ARC --> J2
    WORK[("S3 ghostbus-work<br/>script, wheels, temp,<br/>Athena results")] -.-> J1
    J1 --> ST[("S3 Tables ghostbus-lake<br/>namespaces dim, gold")]
    J2 --> ST
    ST --> CAT["Glue Data Catalog<br/>s3tablescatalog (federated)"] --> ATH["Athena workgroup ghostbus<br/>1 GB scan cap per query"]

    subgraph OBS["alerts"]
        CWM["CloudWatch alarm<br/>no snapshot in 10 min"]
        EVB["EventBridge rule<br/>Glue job FAILED"]
        SNS["SNS topic -> email"]
        CWM --> SNS
        EVB --> SNS
    end
    L1 -.->|"metric Collected"| CWM
    J2 -.-> EVB
    J3 -->|"daily summary<br/>OK / WARN / FAIL"| SNS
```

**Interviewer asks**
- *Why Lambda and not a small EC2 instance for the poller?* No server to patch, no idle cost, and
  the usage fits the always-free allowance. The trade-off is no memory between runs, solved by
  reading state from the archive.
- *Why Glue rather than EMR Serverless?* Both run the same Spark code. The account's Free plan
  denies EMR; Glue 5.1 matches the local Spark 3.5.6 / Iceberg 1.10 exactly (ADR-0010).
- *How does Athena see Iceberg tables in S3 Tables?* The table bucket is federated into the Glue
  Data Catalog as `s3tablescatalog`; Athena queries `"s3tablescatalog/ghostbus-lake".gold.trip_outcomes`.

## 6.1b The public data export

**Say it in one breath:** every night, after the quality checks, a job writes small JSON files
for the website, counting only days that passed the checks, ranking routes conservatively, and
re-uploading only files whose bytes changed.

```mermaid
flowchart TD
    G[("gold.trip_outcomes<br/>last 28 days")] --> A["aggregate: day x route x local hour<br/>(fully collected bus trips only)"]
    Q[("gold.data_quality")] --> P{"day passed<br/>the gates?"}
    A --> B["pure builders<br/>ghostbus_core.export"]
    P -->|"yes"| B
    P -->|"no: listed, never counted"| B
    D[("dim: stops, routes")] --> B
    C[("archive seals")] -->|"chain head"| B
    B --> F["meta.json, days/, league/7d and 28d,<br/>routes/{slug}, stops.json<br/>(sorted keys, fixed rounding)"]
    F --> U{"MD5 equals the<br/>stored ETag?"}
    U -->|"yes"| K["skip: cache stays valid"]
    U -->|"no"| W[("S3 ghostbus-site/data/v1/")]
```

**Interviewer asks**
- *Why static files instead of an API for this?* The site keeps working with every compute part
  switched off, it costs almost nothing to serve, and caches work perfectly because unchanged
  files are byte-identical. Live data ("will my bus come?") is a separate API.
- *How do you stop a bad day reaching the public?* The export reads each day's quality verdict;
  a failed day is listed with a note but counted in no rate, and a property test checks that no
  rate ever includes one.
- *How do you rank routes fairly?* By the lower end of the 95% interval of the untracked rate,
  with at least 30 trips. A small route with 2 bad trips out of 10 can't top the table.
- *How would you change the format without breaking the website?* Never in place: every file
  says `ghostbus.<kind>.v1` and validates against a JSON Schema; a breaking change is `data/v2/`.

## 6.1c "Will my bus come?" (the API)

**Say it in one breath:** a Lambda behind an HTTP API answers "what's coming to my stop, and
will it really come?" from three files in S3: the stop's timetable (a nightly index), the newest
live snapshot the poller already archived, and the route's published history. It never calls
the NTA, and a warm request usually makes one S3 list call and nothing else.

```mermaid
sequenceDiagram
    autonumber
    participant U as Passenger app
    participant G as API Gateway (HTTP API)
    participant L as Lambda ghostbus-api
    participant W as S3 work: api/v1/
    participant A as S3 archive: raw/
    participant S as S3 site: data/v1/routes/
    U->>G: GET /v1/stops/1358/departures
    G->>G: throttle (10/s, burst 20)
    G->>L: invoke
    L->>L: validate the stop code and parameters
    L->>W: current.json, then shard crc32(1358) mod 256 (cached)
    L->>A: list this hour, newest TripUpdates + Vehicles (cached per snapshot)
    L->>S: routes/{slug}.json for each route (cached 1 h)
    L->>L: match, status, expected time, probability
    L-->>U: 200, ghostbus.departures.v1 (cache 30 s)
```

How each departure gets its status and probability (ADR-0012):

```mermaid
flowchart TD
    D["scheduled departure<br/>in the window"] --> F{"live feed under<br/>15 min old?"}
    F -->|"no"| N["no_live_data<br/>p = ran / n"]
    F -->|"yes"| M["match: trip id, else<br/>route + direction + date + start"]
    M --> C{"CANCELED, or this<br/>stop SKIPPED?"}
    C -->|"yes"| Z["cancelled / skips_stop<br/>p = 0"]
    C -->|"no"| P{"vehicle reporting<br/>positions?"}
    P -->|"yes, and past the stop"| X["dropped: already gone"]
    P -->|"yes"| T["tracked<br/>rules out ghost, phantom, early cancel"]
    P -->|"no"| V{"vehicle named?"}
    V -->|"yes"| AS["assigned<br/>rules out ghost, early cancel"]
    V -->|"no"| SC["scheduled<br/>under 15 min away: rules out early cancel"]
    T --> H["p = this hour's history,<br/>shrunk toward the route's rate (20 trips)"]
    AS --> H
    SC --> H
```

**Interviewer asks**
- *Why not stream to the API?* Streaming costs money every hour whether anyone asks or not. The
  poller already writes a snapshot every two minutes, and a snapshot decodes in 6 ms, so the API
  reads the newest one. The answer is at most one poll old, which is how old the data is anyway.
- *How do you hit p95 under 500 ms with S3 behind it?* Caching in the Lambda container: the index
  pointer for 5 minutes, shards and route files until they change, each live snapshot decoded once.
  A warm request is usually one S3 list call. Measured: `docs/measurements/06-api.md`.
- *Why shard the index?* 770,000 departures a day: one file per stop would be 10,000 uploads a
  night; one file would be tens of MB per request. 256 shards by `crc32(code)` is about 260 uploads
  and a ~100 KB read. `crc32`, not Python's `hash()`, which changes between processes.
- *What happens while the nightly index is being written?* Nothing visible: shards go under a new
  build prefix and the pointer `current.json` is written last, so readers switch from one complete
  build to the next (the same idea as a blue/green deploy, for data).
- *Is the probability any good?* It is a stated heuristic (`heuristic-v1`), not yet a measured one.
  Roadmap 2.4 scores it on held-out days (Brier score, reliability diagram) before it is trusted.
  Until a route has published history it answers `null` with the reason, never a guess.

## 6.1d The website

**Say it in one breath:** CloudFront serves static pages from a private S3 bucket, the nightly
JSON next to them, and passes `/v1/*` to the API, so the browser only ever talks to one origin;
every response carries strict security headers.

```mermaid
flowchart TB
    B["visitor's browser"] -->|"HTTPS only"| CF["CloudFront<br/>security headers on every response"]
    CF -->|"/ and /assets/*<br/>cache 5 min"| S3W[("S3 site bucket: pages<br/>private, read through OAC")]
    CF -->|"/data/*<br/>cache 5 min"| S3D[("S3 site bucket: data/v1/<br/>written nightly by Glue")]
    CF -->|"/v1/*<br/>cache 30 s per full URL"| API["HTTP API + Lambda<br/>(6.1c)"]
    TF["terraform apply"] -->|"each file with its MD5"| S3W
```

**Interviewer asks**
- *Why no React or build step?* Four pages and a few hundred lines of JavaScript. A framework would
  add a dependency tree to audit and patch, and a build whose output differs from what was
  reviewed. Plain modules are what ships.
- *Why route the API through CloudFront?* One origin for the browser, so the Content Security
  Policy can say `connect-src 'self'` and no CORS is involved; and CloudFront answers repeat
  questions about a busy stop for 30 seconds, which takes load (and cost) off the Lambda.
- *How do you deploy a change?* `terraform apply` uploads each changed file (it compares MD5s).
  Files carry `max-age=300`, so visitors see the change within 5 minutes without an invalidation.
- *How does the site stay honest before there is data?* It reads the same quality verdicts as the
  export: no published day means an empty league with the reason, never a number from a bad day.

## 6.1e Is the chance any good? (calibration backtest)

**Say it in one breath:** every night a job replays yesterday's archived live feed as it was 30,
15, 5 and 1 minutes before each bus was due, asks the model exactly as the API would have with
history from earlier days only, and scores it against what happened and against two simple rivals.

```mermaid
flowchart TB
    G[("gold.trip_outcomes<br/>28 days before the test day")] --> H["route histories<br/>(same path as the published files)"]
    T[("gold.trip_outcomes<br/>test day, fully collected buses")] --> M["each trip at 30, 15, 5, 1 min<br/>before its scheduled start"]
    A[("raw archive<br/>snapshots of that moment")] --> R["replay + production matcher<br/>(attach_instance)"]
    M --> R
    R --> S["live status then:<br/>cancelled / tracked / assigned / scheduled"]
    H --> P["departures.probability<br/>(the API's code)"]
    S --> P
    P --> Q[("gold.calibration_predictions<br/>day replaced, not appended")]
    T -->|"did it run?"| Q
    Q --> C["Brier vs 'always comes' and base rate;<br/>reliability bins; by lead and status"]
    C --> J["data/v1/calibration.json<br/>drawn on the methodology page"]
```

**Interviewer asks**
- *How do you avoid leaking the answer into the test?* Walk-forward: a test day's predictions use
  history from the days before it only, and the live status comes from snapshots fetched before the
  moment of prediction. The test day's own outcomes are only used to score.
- *Why a Brier score and not accuracy?* Accuracy throws the probability away (is 0.51 "comes"?).
  Brier rewards being confident when right and punishes being confident when wrong, and it is a
  proper scoring rule: you can't game it by shading your forecasts.
- *Why two baselines?* "Always comes" is what a timetable implies, the bar a passenger has today.
  One base rate for every bus is the bar for "the model knows something about this particular bus".
  On the first two days the model beat the first (+7%) and lost to the second (−8%).
- *What did the backtest find?* That 20% of buses showing "cancelled" shortly before departure
  still ran, and that routes with very little history get extreme guesses. Both are fixable
  (shrink routes toward the network rate; learn the chance that a cancelled bus still runs), and
  both would have stayed invisible without replaying the archive (measurement 08).

## 6.2 Who may do what (least privilege)

**Say it in one breath:** every component has its own IAM role with only the actions it needs on
only its own resources, and the account itself sits inside AWS-managed guardrails.

```mermaid
flowchart TB
    subgraph ORG["AWS-managed organization (new sign-up, Free plan)"]
        SCP["service control policies:<br/>only eu-north-1; no EMR, MSK, Kinesis"]
        subgraph ACCT["account <account-id>"]
            ME["IAM user nathan-admin<br/>(CLI access key, laptop only)"]
            RP["role ghostbus-poller:<br/>List archive; Get/Put raw/* and claims/*;<br/>GetParameter on the NTA key only"]
            RW["role ghostbus-static-watcher:<br/>Get/Put raw/feed=static/* only"]
            RS["role ghostbus-scheduler:<br/>InvokeFunction on the two Lambdas"]
            RG["role ghostbus-glue:<br/>read archive raw/*; work bucket;<br/>s3tables:* on ghostbus-lake only"]
            RA["role ghostbus-api:<br/>read-only: live snapshots, stop index,<br/>route files; no secret, no writes"]
            ME ~~~ RP ~~~ RW
            RS ~~~ RG ~~~ RA
        end
    end
    SCP -.->|"caps every principal,<br/>even admins"| ACCT
```

**Interviewer asks**
- *Where is the NTA API key and who can read it?* In SSM as an encrypted SecureString. Only the
  poller role can read that one parameter. It never enters Terraform state: Terraform knows only
  its name, and a script copies it from `.env` without printing it.
- *What is a service control policy?* An organization-level limit that applies to every identity in
  the account, including administrators. The account's first deploy failed with "explicit deny in a
  service control policy"; the IAM policy simulator then mapped exactly which actions and regions
  were allowed, before writing any more Terraform.

## 6.3 Terraform layers and deploy order

**Say it in one breath:** a tiny bootstrap layer (state bucket and budget) is applied once with
local state; everything else lives in the core layer, whose state is stored in that bucket with
locking, and nothing switches on until a flag says so.

```mermaid
flowchart TD
    A["bootstrap (local state)<br/>tfstate bucket, $20 budget alerts"] -->|"output backend.hcl"| B["core (S3 backend + lockfile)"]
    B --> C["archive bucket<br/>prevent_destroy"]
    B --> D["collector: Lambdas, roles,<br/>schedules (collector_enabled)"]
    B --> E["lake: table bucket, catalog,<br/>Glue jobs, triggers (batch_enabled),<br/>Athena workgroup"]
    B --> F["alerts: SNS, alarms,<br/>Glue failure rule"]
    G["build steps before plan:<br/>Lambda zip (arm64), Glue wheels"] -.-> B
```

**Interviewer asks**
- *Why two layers?* Terraform needs somewhere to keep its state before it can create that place.
  The bootstrap layer creates the state bucket with local state; the core layer then uses it.
- *How do you avoid deploying something live by accident?* Features ship disabled behind variables
  (`collector_enabled`, `batch_enabled`), plans are read before every apply, and the irreplaceable
  archive has `prevent_destroy`.
- *How do you keep the Lambda package reproducible?* The build installs Linux arm64 wheels from the
  lock file and writes a zip with fixed timestamps: same inputs, same bytes, so Terraform only
  redeploys on real changes.

## 6.4 Where the money goes

**Say it in one breath:** collection costs about a dollar a month, the nightly batch a few cents a
run, and Kafka plus always-on Spark (about $200 a month) is deliberately not running in the cloud.

```mermaid
flowchart LR
    subgraph ON["always on: ~$1-3 / month"]
        A1["Lambda: inside the free allowance"]
        A2["S3 archive: ~3.3 GB/month of data"]
        A3["S3 requests: ~86k PUTs/month"]
        A4["CloudWatch: 2 alarms, 1 metric, logs (14 days)"]
    end
    subgraph RUN["per run"]
        B1["Glue Flex, 2 x G.1X:<br/>timetable load ~4 cents"]
        B2["Athena: $5 per TB scanned,<br/>capped at 1 GB per query"]
    end
    subgraph OFF["not running (ADR-0009)"]
        C1["MSK + always-on streaming:<br/>~$200 / month"]
    end
    G["guards: $20 budget alerts,<br/>Free plan cannot bill,<br/>features off by default"]
```

**Beginner notes**
- **IAM role**: a set of permissions a service assumes; no passwords or keys are stored in code.
- **Terraform state**: Terraform's record of what it created, compared with the code on every plan.
- **Flex execution**: Glue runs the job on spare capacity for about a third less, starting a little
  later.
- **Federated catalog**: a catalog entry that points at tables managed by another service.
