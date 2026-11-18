# 2. Collection: one API call a minute, never lost

How raw data gets in. This is the part with the hardest rule (one call per minute for the whole
key) and the most interesting reliability questions.

## 2.1 One minute in the life of the cloud poller

**Say it in one breath:** a scheduler wakes a function every minute; the function works out from
the archive whether it may call, claims the minute so nobody else can, calls NTA at second :25,
saves the exact bytes, then writes a heartbeat that records how the call went.

```mermaid
sequenceDiagram
    autonumber
    participant ES as EventBridge Scheduler
    participant L as Lambda poller
    participant S3 as S3 raw archive
    participant SSM as SSM Parameter Store
    participant NTA as NTA GTFS-R API
    participant CW as CloudWatch

    ES->>L: invoke (about 6 s into the minute)
    opt first run in a new container
        L->>SSM: get /ghostbus/nta-api-key (decrypted)
    end
    L->>S3: list + read newest heartbeats (last 30 min)
    Note over L: plan the call: at least 60 s after the last one,<br/>at second :25, after any backoff
    alt the plan is in a later minute
        L-->>ES: skip this minute (late, spacing or backoff)
    else the plan is this minute
        L->>S3: create claims/minute=N only if absent
        alt another invocation already claimed it
            L-->>ES: skip (duplicate delivery)
        else claimed
            Note over L: sleep until :25
            L->>NTA: GET feed for this minute (x-api-key)
            NTA-->>L: protobuf snapshot (about 1 MB)
            L->>S3: put raw/feed=.../dt=/hh=/stamp.pb.zst
            L->>S3: put heartbeat JSON (status, entities, archived)
            L->>CW: log line with metric Collected = 1
        end
    end
```

**Walk through it**
1. The scheduler fires at the start of each minute. The function has no memory, so it reads the
   last half hour of heartbeats to know when the key last called and whether it is backing off.
2. It computes the planned call time with the same pure function the laptop poller uses.
3. It claims the minute with an S3 conditional write (`If-None-Match: *`). Only one writer can
   create the same object, so even two copies of the same trigger cannot both call.
4. Archive first, heartbeat last: a snapshot only counts as "collected" once it is safely in S3.

**Interviewer asks**
- *EventBridge is at-least-once. How do you avoid calling twice?* The S3 conditional write is a
  distributed lock with one winner per minute. The usual alternative, reserved concurrency of 1,
  is often unavailable on new accounts.
- *Why read heartbeats instead of keeping state in DynamoDB?* The heartbeats already have to exist
  (they decide `NO_DATA`), and they also hold the laptop's last call, so the 60 s rule held across
  the move from laptop to cloud with no extra service.
- *Why sleep inside a Lambda? Isn't that paying for nothing?* About 20 s at 256 MB a minute is
  about 216,000 GB-seconds a month, inside the 400,000 always-free allowance. A second scheduler
  just to hit :25 would be more moving parts for no saving.
- *How do you know it's working?* Every collected call logs a CloudWatch metric; an alarm emails
  if fewer than one arrives in 10 minutes.

## 2.2 The pacing rules as a decision flow

**Say it in one breath:** never call twice in 60 seconds, back off after failures, land on the
chosen second, and skip a minute rather than call late.

```mermaid
flowchart TD
    START(["invocation starts at second ~6"]) --> HIST{"any heartbeat in<br/>the last 30 min?"}
    HIST -->|no| FIRST["plan = next :25 after now"]
    HIST -->|yes| FAIL{"last call failed?"}
    FAIL -->|no| SKIP0["skip = 0"]
    FAIL -->|yes| BACK["skip = 0, 1, 3, 7, 15 minutes<br/>by consecutive failures;<br/>HTTP 429 honours Retry-After"]
    SKIP0 --> PLAN
    BACK --> PLAN
    PLAN["plan = :25 of the minute after the last call,<br/>+ skip minutes"] --> SPACE{"at least 60 s after<br/>the last call?"}
    SPACE -->|yes| SAME
    SPACE -->|"no, short by 5 s or less"| JITTER["plan = last call + 60 s"]
    SPACE -->|"no, by more"| REALIGN["plan = one minute later<br/>(back on :25)"]
    JITTER --> SAME
    REALIGN --> SAME
    FIRST --> SAME{"is the plan in<br/>this minute?"}
    SAME -->|no| SKIP(["skip; a later invocation calls"])
    SAME -->|yes| CLAIM{"claim minute in S3"}
    CLAIM -->|lost| SKIP
    CLAIM -->|won| CALL(["sleep until plan, call NTA"])
```

**Interviewer asks**
- *Why no immediate retry on failure?* A retry is a second call in the same minute, which breaks
  the limit. A failed minute is simply a missed snapshot, recorded in its heartbeat.
- *You mentioned a bug here.* Yes: the old plan was "max(offset, last + 60 s)", which locked calls
  on whatever second they first happened at, so moving the offset to :25 had no effect. Found when
  watching the live logs, fixed with the realign rule above, covered by property tests, verified
  live: 07:42:15, then 07:43:25, then every minute at :25.

## 2.3 Which feed each minute

**Say it in one breath:** minute slots alternate between trip updates and vehicle positions, and
one minute in fifteen goes to alerts, so each main feed refreshes every two minutes.

```mermaid
flowchart LR
    M0["slot 0<br/>trip updates"] --> M1["slot 1<br/>vehicles"] --> M2["slot 2<br/>trip updates"] --> M3["..."] --> M13["slot 13<br/>trip updates"] --> M14["slot 14<br/>ALERTS"] --> M15["slot 15<br/>vehicles"] --> M16["slot 16<br/>trip updates"] --> M17["..."]
```

- The rule is a pure function of the clock (`feeds.feed_for_slot`), so the laptop and the Lambda
  pick the same feed for the same minute, and no state is needed.
- The alternation counts only non-alert slots, so neither main feed ever waits more than three
  minutes. An early version left a four-minute gap; a property test caught it.

## 2.4 The static timetable watcher

**Say it in one breath:** every hour, ask NTA whether the timetable file changed (a cheap HEAD
request); if it did, download it, fingerprint it, and store it once under its fingerprint.

```mermaid
flowchart TD
    A(["hourly, at :23"]) --> B["HEAD the static GTFS zip"]
    B --> C{"ETag / Last-Modified / size<br/>same as last time?"}
    C -->|yes| D["update checked_at in _state.json"] --> Z(["done"])
    C -->|no| E["stream download to /tmp,<br/>computing SHA-256"]
    E --> F{"raw/feed=static/sha256=.../manifest.json<br/>already exists?"}
    F -->|yes| G["same bytes seen before:<br/>no new version"]
    F -->|no| H["upload GTFS_Realtime.zip"] --> I["upload manifest.json LAST<br/>(its presence = upload complete)"]
    G --> J["write _state.json"]
    I --> J --> Z
```

**Interviewer asks**
- *Why the SHA-256 and not the file name or date?* Content addressing: identical bytes are stored
  once, and the key proves what is inside. NTA re-uploads every night, sometimes with a new
  timetable version, sometimes with the same one.
- *Why write the manifest last?* It acts as a commit marker. A crash halfway leaves a zip without
  a manifest, which readers ignore and the next run completes.

## 2.5 Cutover: moving the poller from laptop to cloud

**Say it in one breath:** copy the bulk of the archive while the laptop still polls, stop the
laptop, copy the last few files, switch the cloud on, all inside one minute.

```mermaid
flowchart LR
    A["pre-copy archive to S3<br/>(443 MB, 4 min,<br/>laptop still polling)"] --> B["stop laptop poller<br/>07:29:13"]
    B --> C["copy the last minutes<br/>(10 s; includes the laptop's<br/>last heartbeat 07:29:14)"]
    C --> D["terraform apply<br/>schedules ENABLED<br/>07:29:35"]
    D --> E["first cloud call skipped<br/>(scheduler 14.8 s late)<br/>then every minute"]
```

**Interviewer asks**
- *How did you keep the one-call-per-minute rule during the switch?* Only one poller existed at any
  moment, and because the laptop's last heartbeat was in S3 before the Lambda started, the Lambda
  knew exactly when the last call was.
- *What did it cost in data?* About two minutes of `NO_DATA`, which the outcome rules handle
  honestly (never as ghosts).

**Beginner notes**
- **EventBridge Scheduler**: AWS's cron. **At-least-once delivery**: it may occasionally deliver
  the same trigger twice, so receivers must be idempotent.
- **SSM Parameter Store**: a free key-value store for configuration and secrets (SecureString is
  encrypted with KMS).
- **Conditional write**: "create this object only if it does not exist yet". S3 rejects the second
  writer with HTTP 412.
- **Protobuf**: Google's compact binary format; GTFS-Realtime is defined in it.
