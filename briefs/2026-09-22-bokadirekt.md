---
company: "Bokadirekt"
slug: "bokadirekt"
domain: "bokadirekt.se"
demo_date: "2026-09-22"
demo_time: "09:00 America/New_York (15:00 Europe/Stockholm)"
attendees: ["Noel Mattar — technical champion, self-serve Dagster Starter trial user — noel.mattar@bokadirekt.se", "Naomi Mastico — Dagster Labs, self-serve/growth — naomi@dagsterlabs.com (organizer)", "Eric Thomas — Dagster Labs — eric.thomas@dagsterlabs.com", "naomi@prefect.io — also on the invite, role/reason unclear (see Conflicts and gaps)"]
ae_doc: "https://docs.google.com/document/d/1pl-y7zHAff2AaywVJFHaaK-mwAwpclpqxw5UlWIvVGw/edit (support-call transcript, not formal discovery notes)"
ae_doc_modified: "2026-09-09"
overall_confidence: "low"
generated: "2026-09-14"
---

# Bokadirekt — demo brief

## Demo thesis

Noel Mattar is a hands-on technical champion who is *already* self-serve
trialing Dagster: he built his own dbt project on GCP under Dagster Starter,
and hit exactly the wall a free tier is designed to surface — he can't get a
second (production) deployment past Serverless's single-deployment limit,
while trying to merge a branch to prod. He's presenting Dagster to his own
team this month as an investment case. This demo has to prove that the next
tier up removes that one specific blocker — real branch deployments, multiple
environments, and CI/CD gating on a project shaped like the one he already
built — so he leaves with a working answer to his own error message, not a
generic feature tour he has to translate into his environment himself.

## Meeting

- **When:** 2026-09-22, 09:00 America/New_York / 15:00 Europe/Stockholm
- **Who's in the room:** Noel Mattar (Bokadirekt — hands-on dbt/GCP builder,
  title unconfirmed; he said he'll be "presenting to my team next week" as of
  9/9, so he is likely the one championing this internally), Naomi Mastico
  (Dagster Labs, ran the 9/9 support call, organizing this one)
- **Meeting type:** Self-serve upgrade / technical follow-up demo — **not** a
  typical AE-sourced enterprise evaluation. This is a PLG account already
  building on Dagster Starter, escalating toward a paid tier. Flagging this
  prominently because it changes what "demo" should mean here: less
  "convince them Dagster fits," more "unblock the specific wall he hit and
  show the paid-tier capabilities that remove it."

## Use case — confidence: medium

Noel is building his own dbt-on-GCP project against Dagster Starter — a
"clean" (staging) dbt folder and an "entity" (mart) dbt folder, a Python
model, and a dbt model that ingests data. He develops locally, then tries to
merge a branch into a production deployment in Dagster+; the merge appears to
succeed on the branch side but the prod side errors, and Naomi's read (per
the 9/9 call) is that Dagster Starter doesn't support more than one
deployment — i.e., he may be trying to run a second (prod) deployment that
the plan doesn't allow. That is a plan/tier question, not a bug in his code,
and it's the concrete thing this meeting needs to resolve.

## Current stack — confidence: low (see table)

| Layer | Tool | Source |
|---|---|---|
| Orchestration | Dagster Starter (self-serve trial) for the *new* project; what (if anything) orchestrates Bokadirekt's real production pipelines today is unknown | AE call transcript / unknown |
| Warehouse | Likely BigQuery — Noel referred to "this GCP project" while showing his dbt models | inferred, medium confidence |
| Transformation | dbt (Core) — confirmed directly, he showed his own dbt models, ran `dbt build`-style commands locally | AE call transcript, high confidence |
| Ingestion | Unknown — one dbt model "ingests some data" but that may just be a source-reading model, not a dedicated ingestion tool | unknown |
| BI | Unknown | unknown |
| Cloud | GCP | AE call transcript, medium-high confidence |
| CI/CD | Git branch → Dagster+ Serverless branch deployment → merge to prod — this is the exact workflow he's stuck on | AE call transcript, high confidence |

## Pain — confidence: high (that this specific pain is real; low on how representative it is of anything beyond his own account)

Direct from the 9/9 call transcript:

> "I've tried to debug it from every angle... this is something [that] blocks
> me from actually trying out this tool."

> "I would see this as an investment... they are very excited when it comes
> to Dagster... probably we will end up with Daxter [Dagster] in the future
> in the coming months."

Naomi's working theory on the call: "that might be because you can't have
multiple deployments in Dagster Starter." Noel also hit a CLI login/redirect
issue (`dagster-cloud` or `dg` login via Google SSO redirecting oddly) that
neither of them resolved live.

## Data domains

Unconfirmed. Bokadirekt's actual business is a health & beauty booking
marketplace (appointments, specialists/service providers, consumers,
payments), so those are the plausible real-world domains — but Noel's sandbox
project may be using placeholder/demo data unrelated to production (the GCP
project name he mentioned was garbled in the transcript, transcribed as
something like "book[a] nail democracy," which doesn't parse cleanly — treat
as unknown, not as a confirmed project name). **This is an assumption, not a
confirmed fact** — flagged per house rules.

## Public signals

- **Job postings:** Bokadirekt currently has two open Data & Analytics roles
  in Stockholm (hybrid): **Data Engineer** and **Product/Conversion Analyst**.
  One source describes this as their first Engineering & Technical hiring in
  about 12 months — a small, currently-quiet data function that appears to be
  re-investing now. Full job description text wasn't accessible (careers page
  blocked to this session), so no specific tools could be confirmed from the
  posting itself.
- **Engineering blog / GitHub:** `github.com/bokadirekt` has ~8 public repos,
  all mobile-consumer-app dependency shims (Klarna mobile SDK, Adyen 3DS2/iOS,
  Amplitude Swift, CommonMark Java, SwiftyMarkdown, Mopinion SDK) — no data
  engineering repos, no engineering blog found.
- **Recent news:** Founded 2009, Stockholm HQ, ~125 employees, ~$35M revenue
  (2024 figure). Series C in Dec 2021: $32.7M / SEK 300M led by VNV Global and
  Sprints Capital, valuing the company at ~SEK 1B. 14,000+ affiliated
  specialists nationwide, millions of monthly visits/bookings. No 2026 funding
  or leadership-change news found.
- **Industry / compliance:** Swedish company processing EU consumer personal
  data (bookings, contact info, likely payment references via
  Klarna/Adyen) — GDPR applies as a baseline. No Bokadirekt-specific privacy
  or compliance initiative found publicly.
- **Orchestration signals:** None publicly. The only orchestration signal at
  all is Noel's own hands-on Dagster Starter trial (see Use case/Pain above).
  Competitors in their market (Fresha, Booksy, Vagaro, Treatwell) are noted
  for context only — not relevant to their internal data stack.

## Conflicts and gaps

- **No formal AE discovery notes exist for this account.** The only input
  material is a support-call transcript between Noel Mattar and Naomi
  Mastico from a self-serve trial, not a discovery call with an AE. Treat
  every "use case / pain / stack" claim above as coming from that one call,
  not from a qualified sales process.
- **`naomi@prefect.io` is on the calendar invite** alongside
  `naomi@dagsterlabs.com` for the same first name — this pattern also
  appears on unrelated internal recurring meetings on my calendar (e.g.
  "Eric / Izzy," which pairs `eric.thomas@dagsterlabs.com`/`eric@prefect.io`
  and `izzy@dagsterlabs.com`/`izzy@prefect.io`). It looks like a calendar
  sync artifact rather than a real second attendee — flagging rather than
  silently dropping it, since I can't confirm either way.
- **Whether Bokadirekt has any production orchestrator today is unknown.**
  Noel's project reads as a green-field personal evaluation, not a stated
  migration off an incumbent tool. Don't build a "replacing X" narrative —
  there's no evidence for one.
- **The exact reason for his deployment error is unconfirmed.** Naomi's
  "Starter doesn't support multiple deployments" theory was not verified on
  the call. Build the demo to *show* what a paid tier's branch-deployment /
  multi-environment model looks like working end-to-end; don't assert his
  specific error's root cause as fact in the demo script.

---

# Build directives

## Asset graph

Small and shaped to mirror what Noel already built, not a large synthetic
estate — this is a self-serve account, not a platform-consolidation pitch.
~7 assets:

- **Staging layer** ("clean," matching his own folder name): 2 dbt staging
  models — `stg_bookings`, `stg_specialists` — reading from a
  BigQuery-badged raw source.
- **Entity layer** ("entity," matching his folder name): 2 dbt mart models —
  `dim_specialists`, `fct_bookings_daily` (daily-partitioned, matching a
  booking-cadence business).
- **One ingestion-shaped asset**: `raw_bookings_feed` (external/observable
  source asset, standing in for "the model that ingests data" he described).
- **One Python-authored rollup** (Axis 2, stubbed by default): a small
  derived asset, e.g. `specialist_utilization_daily`, to demonstrate a
  non-dbt Python asset living alongside the dbt project the way his own
  project does.

Daily partitioning on the booking-facing assets — appointment bookings are
naturally daily-cadence for this business.

## Fidelity

**Stubbed (default).** Axis 2 bodies (`specialist_utilization_daily`) are
`pass`. Real dbt SQL runs for the staging/entity layer regardless, per house
rules. Nothing here calls for data-backed fidelity — no specific number is
part of the pitch.

## Demo shape

**Build-a-pipeline**, not orchestrate-existing or migration-both-states.
There's no stated legacy orchestrator to show alongside this. Keep it a
straightforward dbt-on-BigQuery(-badged) pipeline.

## Native integrations to use

- `dagster-dbt` — confirmed directly (he showed his own dbt project).
- BigQuery kind badging on the raw/staging assets (`kinds={"bigquery"}`) —
  **medium-confidence assumption**, since he said "GCP project" but never
  said BigQuery by name. If the build routine wants to hedge, a generic
  GCP-flavored badge is safer than guessing a different specific warehouse.
- No Snowflake, no Fivetran, no Airflow, no BI tool — none were named or
  evidenced. Do not add any of them.

## Community components to search for

None expected. This is core `dagster-dbt` plus a BigQuery-flavored I/O seam
— no external job/workspace system in scope. Search anyway per the escalation
ladder (`dagster-component search bigquery`, `search dbt`) before assuming,
but I'd be surprised if rung 4 is needed here.

## Asset checks

At least 3, mapped to the concrete pain points in the room:

1. **Blocking** — `stg_bookings` completeness check (row-count-not-zero /
   no-null-booking-id): answers "what happens when the ingest is broken,"
   the exact class of problem a self-serve user building their first
   pipeline worries about.
2. **Warning** — schema/PII shape check on `stg_specialists` (expected
   columns present, no unexpected PII column) — ties to the GDPR context
   noted above.
3. **Blocking** — `fct_bookings_daily` freshness-adjacent completeness check
   gating the rollup asset, so a bad day's data doesn't silently roll
   forward.

## Demo mode

- **What must be mocked:** BigQuery credentials/connection — no real GCP
  access for this account. Follow `templates/demo_mode_pattern.py`: real
  I/O against DuckDB locally, `bigquery` kind badge for visual fidelity.
- **What can be real:** dbt Core (real models, real `schema.yml` tests),
  local DuckDB storage.
- **Asset kinds to display:** `bigquery`, `dbt`.
- **Everything materializes green.** No planted failure. The whole point of
  this meeting is showing a clean branch-deploy → merge → prod flow, which
  is the opposite of what he experienced — don't undercut that by staging a
  break somewhere else in the graph.
- **Data realism notes:** small, deterministic row counts — this is a
  walkthrough for one technical person, not a volume story.

## Three buckets

- **In code:** the dbt staging/entity project mirroring his own folder
  structure, the ingestion-shaped asset, the Python rollup, 3 asset checks,
  one freshness policy on `fct_bookings_daily`, eager automation on the
  entity layer.
- **Dagster+ (demonstrate, don't build):** **branch deployments and
  multiple environments** — this is the actual fix for his blocker, and it
  is a platform capability, not something this repo should hand-roll. Also:
  CI/CD gating on PRs, alerting to Slack, RBAC/teams (relevant if he's about
  to bring his team in), restart-from-failure.
- **Conversation only:** anything about Bokadirekt's real production data
  estate (unknown), scaling this pattern to their actual specialist/booking
  volumes, any migration off an incumbent orchestrator (no evidence one
  exists).

## Demo name

**"Ship the Second Deployment"** — names the actual blocker this meeting
exists to resolve.

## Money shot

Push a branch with a small dbt model change, show the Dagster+ branch
deployment build and preview it end-to-end, then merge to a *second,
independent* production deployment and watch it deploy cleanly alongside the
branch one — directly answering the error he hit on his own trial. Then walk
the green graph: staging → entity → rollup, checks passing, freshness policy
visible on `fct_bookings_daily`.

## Capability talk track

- **Branch deployments / multiple environments (Dagster+, bucket 2):** the
  headline moment — this is the plan-tier capability that was missing under
  Starter.
- **Asset checks (bucket 1, built):** blocking check on `stg_bookings` — "if
  your ingest model comes back empty, this stops before it reaches your
  mart, instead of silently rolling forward."
- **Freshness policy (bucket 1, built):** on `fct_bookings_daily` — "here's
  how you'd know if this stopped updating, without checking manually."
- **Automation conditions (bucket 1, built):** eager on the entity layer —
  contrast with manually re-running `dbt build` locally.
- **Alerting to Slack (bucket 2, mention only):** don't build; describe it
  as a config toggle away, relevant since he'll be looping in a team.

## Explicitly out of scope

- Anything about Bokadirekt's real production pipelines — we don't know what
  they run today, so don't invent a "replacing X" story.
- Payment processing detail (Klarna/Adyen) — those are consumer-app
  dependencies, not evidenced as part of any data pipeline.
- Large-scale/multi-region framing — this is a single technical champion's
  own project, not a platform-consolidation pitch. Keep the graph small.
