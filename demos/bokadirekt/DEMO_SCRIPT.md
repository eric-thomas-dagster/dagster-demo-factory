# DEMO_SCRIPT — "Ship the Second Deployment" (Bokadirekt)

Technical follow-up for Noel Mattar (Bokadirekt — hands-on dbt/GCP
builder, self-serve Dagster Starter trial user) and Naomi Mastico (Dagster
Labs, ran the original 9/9 support call). This is **not** an enterprise
evaluation — it's unblocking one specific technical champion who is about
to present Dagster to his own team. Everything today should map back to
his own error message.

## 0. Open by naming his own blocker back to him

Say: *"On the call with Naomi, the issue was that your branch merge to
prod errored — Starter caps you at one deployment. That's the entire
agenda today: show you the workflow with a second deployment available,
on a project shaped like the one you already built."*

## 1. The asset graph — his own project shape, not a generic tour

1. Open the full lineage graph. Say: *"This mirrors what you already
   built — a 'clean' staging folder, an 'entity' mart folder, a Python
   model, and one feed standing in for the ingestion model you
   described."*
2. Click `raw_bookings_feed`. Say: *"This is observed, not materialized —
   your booking platform owns this data; Dagster just watches it for
   freshness, the same shape as the ingestion model in your own
   project."*
3. Click through `stg_bookings` → `dim_specialists` / `fct_bookings_daily`
   → `specialist_utilization_daily`. Say: *"Same two dbt folders you
   already have, plus your Python model, all in one lineage graph."*

## 2. The money shot — branch deployment → second production deployment

This is almost entirely a Dagster+ UI walkthrough, not a code change in
this repo.

1. Make a small, visible change to a dbt model (e.g. add a column to
   `stg_bookings.sql`) on a new git branch, push it.
2. In Dagster+, show the **branch deployment** building automatically off
   that push. Say: *"This is exactly the workflow you were already doing
   — push a branch, Dagster+ builds an isolated deployment for it."*
3. Open the branch deployment's code location — same asset graph,
   isolated. Materialize `stg_bookings` there to prove it runs cleanly in
   isolation.
4. Merge the branch. Show the merge landing in a **second, independent
   production deployment** — not erroring, not colliding with the first.
   Say: *"This is the exact step that failed for you on Starter. On this
   tier, it's just... a deployment. Nothing special happened just now,
   and that's the point."*
5. Walk the green graph end to end: staging → entity → rollup, checks
   passing, freshness policy visible on `fct_bookings_daily`.

## 3. Asset checks — what happens when the ingest is broken

1. Click `stg_bookings`'s metadata panel. Point at
   `stg_bookings_completeness` — passing, blocking. Say: *"If your ingest
   model comes back empty or with a null booking ID, this stops right
   here — the entity layer never sees it, instead of silently rolling
   forward a bad day."*
2. Click `stg_specialists`. Point at `stg_specialists_schema_pii` —
   warning severity. Say: *"This is a GDPR guardrail — if an unexpected
   PII-shaped column shows up in your specialist roster, this flags it
   without hard-stopping your pipeline over something that needs a human
   look, not an automatic halt."*

## 4. Freshness and automation — how you'd know something broke

1. Open `fct_bookings_daily`'s metadata panel. Point at the **freshness
   policy**. Say: *"This is how you'd know something stopped updating,
   without checking manually — the direct contrast to re-running `dbt
   build` locally to see if anything changed."*
2. Open `dim_specialists`'s or `stg_bookings`'s automation condition.
   Say: *"This recomputes the moment upstream data lands, gated on the
   completeness check passing — no cron job, no manual trigger."*
3. Toggle the `raw_bookings_feed_observation_sensor` on in the Sensors
   tab. Say: *"This polls your booking platform's own export, so a
   booking that lands outside anything Dagster triggered still shows up
   as an observation — 'what happens when it wasn't Dagster that updated
   this' is always the next question."*

## 5. What Dagster+ adds on top (don't build it live — point at it)

Say: *"Branch deployments were the headline fix for today, but there's
more that's relevant once you bring your team in next week."*

1. **Alerting to Slack** — a config toggle, not code, once he has a team
   channel to route to.
2. **RBAC / teams** — relevant the moment more than one person touches
   this project.
3. **Restart-from-failure, run history** — the operational layer around
   the OSS project he already knows.

## 6. Close

Say: *"Everything you saw today runs identically against your real
BigQuery project — flipping it on is a `profiles.yml` target change, not
a rewrite. The actual fix for what blocked you is the tier itself, not
anything in this code."*

Then, to Noel directly: *"You said you're presenting this to your team
next week — what do you need from today to make that conversation land?"*

## What's out of scope today (say if asked, don't volunteer)

- No claim about Bokadirekt's real production data estate — nothing here
  represents what (if anything) orchestrates their actual booking
  pipelines today; this is Noel's own sandbox project, not a migration
  story.
- No real BigQuery — the badge reflects "this GCP project," not a
  confirmed warehouse; be direct that DuckDB stands in for it today.
- No payment-processing detail (Klarna/Adyen) — not part of this data
  pipeline.
- No planted failure anywhere — every check, freshness policy, and
  automation condition shown is real and currently green; the whole
  point of this meeting is the opposite of the error Noel hit.
