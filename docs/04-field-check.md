# Field check: does "untracked" mean the bus never came?

The feeds can say a bus was never tracked. They cannot say it never came: a bus with a broken
tracker still carries passengers. The only way to measure how often "untracked" is wrong is to
stand at a stop and look. About 50 observations turn the ghost numbers from a dashboard into
evidence, so this is the most valuable two hours of the project.

## What to do

1. **Pick 2 or 3 stops** with frequent buses from at least two operators, ideally with a real-time
   display. Note each stop's **plate number** (the number on the pole, the `stop_code`).
2. **Go at a busy time** (07:30 to 09:30 or 16:30 to 18:30 on a weekday), while the collector is
   running on the laptop (Docker Desktop open, `docker compose ... --profile collect` up).
3. **For each scheduled departure** at the stop (from the TFI app or the timetable at the stop),
   record one row in a copy of [`field_check/observations_template.csv`](../field_check/observations_template.csv):

| Column | Meaning |
|---|---|
| `date` | `YYYY-MM-DD` |
| `stop_code` | Plate number on the pole |
| `route` | Route as shown on the bus, e.g. `46A` |
| `scheduled_time` | Timetabled departure at this stop, local time `HH:MM` |
| `observed` | `arrived`, `no_show`, or `unsure` (you left early, could not see, ...) |
| `observed_time` | When it actually came, `HH:MM`, if it came |
| `board_showed` | What the display or app showed: `live`, `scheduled`, `cancelled`, `nothing` |
| `notes` | Anything odd |

Write the row when you are sure: a bus that has not come 20 minutes after its time and is no
longer on the board is a `no_show`. Never guess; `unsure` rows are dropped.

4. Save the file as `field_check/observations-YYYY-MM-DD.csv` (it is git-ignored: it says where
   you were and when) and run, a couple of hours later (trips are decided an hour after their
   scheduled end, and reconcile must have run):

```bash
docker compose -f local/docker-compose.yml run --rm spark python -m ghostbus_jobs.reconcile --from 2026-11-18 --to 2026-11-18
docker compose -f local/docker-compose.yml run --rm -v ./field_check:/field_check spark python -m ghostbus_jobs.field_check /field_check/observations-2026-11-18.csv
```

## What it reports

Each observation is matched to the scheduled trip that serves that stop on that route within
three minutes of the time you wrote, then compared with the outcome we computed:

- **Agreement:** you saw it arrive and we said RAN; you saw a no-show and we said GHOST,
  PHANTOM_LIVE, VANISHED or CANCELLED.
- **The key number: of the trips we called untracked, how many did you see arrive?** That is the
  share of "ghosts" that were really buses with broken trackers. It is published next to every
  ghost rate.
- **Missed ghosts:** buses that did not come but we called RAN (should be close to zero).

Fifty observations give an interval of roughly plus or minus 10 points on these shares. That is
rough, but it is measured, and it is stated as rough.
