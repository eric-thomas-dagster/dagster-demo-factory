---
company: "Neighborhood Intelligence (formerly Bed Bath & Beyond; brands: Beyond, Overstock, buybuy BABY, Kirkland's, The Container Store, Elfa, Closet Works)"
slug: "neighborhood-intelligence"
domain: "beyond.com"
demo_date: "2026-09-16"
demo_time: "13:00-14:30 America/Chicago (organizer's calendar shows a raw -04:00 offset alongside an America/Chicago tz field — treat 13:00 Central as the working assumption and confirm with Sam Garcia if it matters)"
attendees:
  - "Leigha Hubbard — VP of Data, Neighborhood Intelligence — lhubbard@beyond.com (optional on the invite)"
  - "Suhail Memon — Director of Data Platforms, Neighborhood Intelligence (10+ years at the company) — suhailmemon@beyond.com — wrote the demo ask, primary audience"
  - "gmadireddy@beyond.com, mgupta@beyond.com, dipalpatel@beyond.com, ngassmann@beyond.com, svinnakota@beyond.com, dennis.jones@beyond.com — Beyond-side data engineers/platform staff, titles not in the invite"
  - "kwbetters@containerstore.com, vmongia@containerstore.com, tviers@containerstore.com — Container Store-side data staff, titles not in the invite"
  - "samuel.garcia@dagsterlabs.com — AE, organizer"
ae_doc: "https://docs.google.com/document/d/1pUucF1fnT5rGx5onD3OHL4NxG3NQ9_DzHK5MN5Rq7nc/edit"
ae_doc_modified: "2026-09-15T17:15:31Z (shared with Eric same day; created 2026-09-14)"
overall_confidence: "high"
generated: "2026-09-15T19:00:00Z"
---

# Neighborhood Intelligence — demo brief

## Demo thesis

Neighborhood Intelligence's data platform team is mid-consolidation — folding Beyond,
Overstock, The Container Store, and Kirkland's onto a single BigQuery warehouse while
still running roughly 3,000 Control-M jobs and still deciding on a transformation/
ingestion layer — under real executive pressure to standardize now. Suhail Memon
explicitly does not want a feature tour: he wants to watch someone build a DAG by hand
in Dagster, then watch the *identical* pipeline get rebuilt by prompting Claude via MCP
with no ticket, because the company's own stated strategy is agentic-first (CEO Marcus
Lemonis is shipping a customer-facing AI agent, "Norm," this year) and Suhail is
explicitly weighing "legacy tools bolting on MCP" against tools that are agentic-native.
This demo has to prove two things at once: that Dagster's asset model turns a
BigQuery bronze→silver→gold→report pipeline into something buildable by hand in
minutes, and that an LLM can build the same graph from natural language alone — with
Control-M shown coexisting at the edge, not replaced, since a large share of those
3,000 jobs will stay put regardless of what wins here. If this lands, the stated next
step is a paid POC plus a full training day, so this room is really auditioning for a
multi-week engagement, not just taking a look.

## Meeting

- **When:** 2026-09-16, 90 minutes (calendar shows 13:00–14:30, timezone field says
  America/Chicago — matches Suhail's own request for a 90-minute slot).
- **Who's in the room:** Leigha Hubbard (VP of Data — cares about cross-brand
  standardization, executive optics, and not creating ungoverned shadow-BI); Suhail
  Memon (Director of Data Platforms, wrote the ask — cares about hands-on engineering
  reality, whether his team can actually build with this day one, and wants Dagster's
  product team to hear AI/agentic roadmap feedback); several unnamed Beyond and
  Container Store data engineers (the "mixed Python proficiency" cohort the pain
  section calls out directly).
- **Meeting type:** First technical demo, explicitly build-focused rather than a
  feature tour. Two-part live-build session per the AE handoff (see Build directives).

## Use case — confidence: high

Neighborhood Intelligence is running a company-wide "everything home" roll-up:
Bed Bath & Beyond's holding company has acquired Overstock, Kirkland's, and The
Container Store (plus Elfa and Closet Works), and each brand arrived with its own
legacy ERP and data stack. They've picked BigQuery as the target warehouse and are
now selecting the orchestration layer to standardize on across brands, alongside a
still-undecided transformation tool (dbt under evaluation) and ingestion tool (dlt or
Informatica under evaluation). Control-M (legacy, ~3,000 jobs, 10+ years at Beyond)
and RunMyJobs by Redwood (SAP-based, used at some other portfolio brands) are the
incumbent schedulers this has to coexist with or replace.

## Current stack — confidence: high (warehouse, legacy orchestration) / medium (transformation, ingestion)

| Layer | Tool | Source |
|---|---|---|
| Orchestration (legacy) | Control-M (~3,000 jobs, primary at Beyond), RunMyJobs by Redwood (SAP-based, other portfolio brands) | AE notes, high confidence |
| Orchestration (evaluating) | Dagster (this meeting), no named competitor | AE notes |
| Warehouse | BigQuery — "already selected" | AE notes, high confidence |
| Transformation | dbt — under evaluation, not committed | AE notes, medium confidence |
| Ingestion | dlt or Informatica — under evaluation, not committed | AE notes, medium confidence |
| BI | Not named | unknown |
| Cloud | Google Cloud (implied by BigQuery) | inferred, medium |
| CI/CD | "Fully CI/CD-automated environment promotion with Git integration already in place" | AE notes, high confidence |

## Pain — confidence: high

Direct from the AE's discovery notes, in the prospect's own framing:

- Disparate legacy data sources across acquired brands (Beyond, Overstock, Container
  Store, Kirkland's) → no end-to-end visibility; Leigha and Suhail can't see how data
  flows across the full pipeline.
- Job-centric orchestration (Control-M) instead of asset-centric → troubleshooting
  means manually disassembling and retracing dependencies; wasted engineering time and
  compute re-running whole pipelines instead of just the failed step.
- No quality/freshness checks tied to individual pipeline stops → "a job can show
  green while still delivering stale or wrong-row-count data," so problems go
  undetected until downstream teams notice.
- Downstream BI/analytics/ops teams lack self-serve visibility → repeated pings to
  Suhail's team instead of self-resolving; an ongoing support burden.
- No confidence in data quality once it leaves the data team's purview.
- Desire to decentralize analytics to business teams, balanced against fear of a
  "shadow" stack connecting directly to BigQuery outside any governance.
- Morning batch jobs sometimes run late → cascading SLA misses (a 9am executive
  report doesn't land on time) with no predictive ETA capability today.
- Mixed Python proficiency among data engineers inherited from acquired companies →
  drives the desire to standardize and "index for" Python/AI fluency over time.

**Critical event (AE notes, high confidence):** downward pressure from executive
stakeholders to standardize now. **Cross-check (public data, medium confidence,
not stated by the AE):** Neighborhood Intelligence's Q1 2026 earnings call included
a stated plan to cut another $60M in costs over nine months through an AI-driven
headcount reduction — this is consistent with, and likely part of, the executive
pressure the AE notes describe, though the AE notes never mention cost-cutting
directly. Worth knowing before the room, not worth saying out loud unprompted.

## Data domains

Neighborhood Intelligence's real domains are retail orders/inventory across four
brands, loyalty/customer identity (a live Bilt Guest Experience Platform integration
unifying identity across Beyond, Container Store, Kirkland's, Overstock, and buybuy
BABY), and brand-specific ERP extracts (some SAP-based, scheduled via RunMyJobs).

**The demo's own dataset is deliberately NOT this domain.** Suhail's ask names "a
simple dataset (e.g., NYC taxi/cab data)" explicitly — this is a build-mechanics
session, not a narrative-fit session, and he wants the mechanics uncluttered by
domain modeling. Build directives below follow that instruction rather than the
house style of pulling nouns from the AE's discovery notes; the one exception is the
Control-M coexistence vignette, which should use their real vocabulary since that
part of the ask *is* about their actual legacy estate.

## Public signals

- **Job postings:** No Neighborhood Intelligence / Beyond Inc data-engineering job
  postings turned up in searches of Indeed, Glassdoor, LinkedIn, or Built In — either
  not currently hiring for the role or not indexed at search time. Treat the AE notes
  as the only stack evidence; do not substitute a guessed stack from generic postings.
- **Engineering blog / GitHub:** None found under Beyond Inc, Neighborhood
  Intelligence, or Bed Bath & Beyond names.
- **Recent news:**
  - Company completed its rebrand from Bed Bath & Beyond, Inc. to Neighborhood
    Intelligence, Inc. in August 2026, moving from NYSE to Nasdaq under ticker
    **NXH**. CEO/Executive Chairman is Marcus Lemonis.
  - Q2 2026: revenue $361.2M (+28% YoY), net loss $39.5M (105% worse YoY), EPS missed
    estimates by 46%. Kyla Robinson holds the title "Chief Technology Transformation
    Officer."
  - Company frames itself around three pillars — omnichannel retail, home services,
    home ownership — with a technology/data platform positioned as the company's
    "operating system."
  - Stated AI strategy: a proprietary customer-facing AI agent, "Norm," with a first
    version planned for later in 2026. This directly supports the AE notes' claim
    that Suhail is explicitly evaluating vendors on agentic-native vs. bolted-on-MCP
    terms — it's not just his personal framing, it's company strategy from the CEO.
  - Recently closed acquisitions consolidating the "Everything Home" portfolio: The
    Container Store (~$150M, closed 2026, includes Elfa and Closet Works), Kirkland's
    brand IP (~$5–10M, closed 2025). Each brings its own legacy ERP/data stack,
    corroborating the AE notes' "disparate legacy sources" pain directly.
  - Partnership with Bilt: the "Guest Experience Platform" acts as a shared
    intelligence layer (unified customer identity, loyalty, engagement) across all
    portfolo brands, built on a real-time context layer powered by Materialize, and
    integrating with Verifone at the point of sale. Not orchestration-relevant, but
    shows the company already operates modern streaming/data infrastructure alongside
    its legacy Control-M estate.
  - Q1 2026 earnings call: plan to cut ~$60M in costs over nine months via an
    AI-driven headcount reduction (see Pain section cross-check above).
- **Industry / compliance:** Retail with an active payments/loyalty integration
  (Bilt/Verifone) — PCI-DSS relevance is likely (medium confidence, not stated by AE).
  Multi-brand customer data unification also raises state privacy law exposure
  (CCPA-class, medium confidence). Neither was mentioned in the AE notes; do not
  present either as confirmed in the room.
- **Orchestration signals:** No public evidence of Airflow, Prefect, or any other
  named competitor in this evaluation. Treat "who else is in the deal" as unknown —
  the AE notes don't name a competitor either.

## Conflicts and gaps

No direct conflicts between the AE notes and public research — public data
corroborates the AE notes closely (BigQuery-centric consolidation story, disparate
legacy brand stacks, agentic-AI company strategy). The one thing worth flagging as an
**inference, not a stated fact**: the "downward pressure from Executive stakeholders"
the AE notes cite is plausibly connected to the company's public cost-cutting plan,
but the AE notes present it as a standardization mandate, not a cost mandate — treat
both framings as live in the room rather than assuming which one Leigha will lead with.

**Unknown, not filled in:** competitive context, BI tool, cloud provider beyond the
BigQuery inference, and exact timezone for the meeting (calendar data is internally
inconsistent — see Meeting section).

---

# Build directives

## Asset graph

**This demo has a different job than every prior build in this repo: the ask is not
"show me a finished pipeline," it's "watch me build the pipeline, twice — once by
hand, once by prompting an LLM."** Size and structure the graph for live buildability
in a 90-minute room, not for looking comprehensive.

Build **one small canonical pipeline** (5 assets) using NYC taxi/cab data as the
substrate, per Suhail's explicit instruction:

- `raw_taxi_trips` — bronze. BigQuery-badged (see Native integrations). Daily
  partitioned, small deterministic seed (a few thousand rows across ~14 partition
  days is plenty — this needs to look reasonable rematerializing live, not be large).
- `zone_lookup` — bronze. Static reference dimension (NYC taxi zone IDs → borough/
  zone names), unpartitioned.
- `stg_taxi_trips` — silver. Cleaned/typed/joined against `zone_lookup`. Daily
  partitioned.
- `daily_zone_revenue` — gold. Aggregated by zone and day. Daily partitioned.
- `taxi_revenue_report` — report layer. The "final report" Suhail's notes call out by
  name, unpartitioned (rolls up the trailing window).

Then build a **separate, small, pre-built (not live-built) coexistence vignette**
answering ask #4 directly, using their *real* vocabulary since this part is about
their actual legacy estate, not the build mechanics:

- `control_m_nightly_inventory_sync` — external `AssetSpec`, representing a
  Control-M-triggered nightly inventory extract from one acquired brand (e.g.
  Container Store). Never triggered or executed by Dagster.
- `inventory_snapshot_bigquery` — a genuine Dagster-owned asset downstream of it,
  landing the extract into BigQuery. This is the one point in the graph where the
  legacy-scheduled and Dagster-owned sides visibly meet — tag it
  `integration_pattern: coexistence` and `legacy_system_boundary` per the house
  convention.

7 assets total. Do not add more — a thin, fast, reliable graph is the whole point when
half of it gets rebuilt live in front of the room twice.

**Ship two states of the same project, not two projects:** deploy the fully-built
7-asset graph as the project of record (this is what gets validated, deployed to
Dagster+, and proves the thing is real). Additionally, document a checkpoint path in
the README/DEMO_SCRIPT — e.g. a `starter` git tag or branch at the point where only
`raw_taxi_trips`, `zone_lookup`, and the coexistence vignette exist — so Eric can reset
to that state live, walk through building `stg_taxi_trips` → `daily_zone_revenue` →
`taxi_revenue_report` by hand, then reset again and rebuild the same three assets by
prompting Claude via MCP against the identical starter state. Say explicitly in the
README which git ref is the starter checkpoint.

**Make sure this project's `.claude/skills/` includes `dagster-expert` and
`dignified-python`** (copy from this repo's root, same as every other generated
project) — the AI-driven half of the demo is prompting Claude Code against this exact
project, and it should visibly use Dagster's own skills rather than improvise the API
surface. This is directly responsive to Suhail's "Dagster's Claude skills" phrase.

## Fidelity

**Data-backed**, not stubbed, for the taxi pipeline — this is the one departure from
the usual default. The whole point of the live build is that clicking materialize
produces real row counts on real (if small) data, both when built by hand and when
built by Claude. Stubbed `pass` bodies would make the live-build story hollow. The
coexistence vignette can stay stub-level (`inventory_snapshot_bigquery` doesn't need
real inventory numbers) since it's a conversation prop, not something rebuilt live.

## Demo shape

Hybrid: **build-a-pipeline** (primary — the live-build session) with a small
**orchestrate-existing-workloads / coexistence** vignette (pattern 4) layered in for
ask #4. Not a migration-both-states story — Neighborhood Intelligence isn't mid-
migration off one named legacy platform, they're consolidating several onto one, and
only the Control-M piece needs representing here.

## Native integrations to use

- **`dagster-gcp`'s BigQuery IO manager** — real, native, badges the taxi pipeline
  `bigquery` correctly. Subclass it for the demo-mode DuckDB swap per
  `templates/demo_mode_pattern.py` (same resource-seam pattern as partners-fcu's
  `mssql_io_manager` subclass) — `demo_mode: true` routes to DuckDB locally,
  `demo_mode: false` is the unmodified registry component against real BigQuery.
- No dbt. It's explicitly "under evaluation," not adopted — do not build a dbt
  project on spec for a tool they haven't chosen. Build the transformation layer as
  native Dagster assets (Python/SQL against the same IO manager) instead.
- No ingestion tool (dlt/Informatica also undecided) — `raw_taxi_trips` is a directly
  seeded/observed source asset, not routed through a mocked ingestion connector.

## Community components to search for

Search before assuming a gap exists, per the escalation ladder:

- `bigquery`, `google cloud bigquery` — confirm nothing in the community registry
  beats the native `dagster-gcp` component for this use (it shouldn't — native wins).
- `control-m`, `redwood runmyjobs`, `sap job scheduler`, `legacy scheduler
  observation` — very likely nothing exists for observing Control-M or RunMyJobs run
  history. If confirmed empty, that's a real `component-feedback/` entry (Control-M is
  a common enough legacy scheduler that its absence is worth recording), and the
  coexistence vignette's `AssetSpec` + sensor pattern is the correct rung-4 fallback in
  the meantime — don't force a custom "Control-M component" into existence for one
  external asset.

## Asset checks

At least three, each mapped to a named pain:

1. **Blocking** — `raw_taxi_trips` row-count / non-null completeness check. Directly
   answers "a job can show green while still delivering stale or wrong-row-count
   data" — gate `stg_taxi_trips` on it.
2. **Blocking** — `stg_taxi_trips` schema/type validity check (fare/zone values in
   expected ranges). Reinforces the same pain from the transformation side.
3. **Warning** — `daily_zone_revenue` reconciliation check (aggregated total against
   `stg_taxi_trips` sum for the same partition). Answers "no confidence in data
   quality once it leaves the data team's purview" — this is the check a downstream
   BI consumer would want to see exists.

## Demo mode

- **What must be mocked:** BigQuery itself (no real warehouse credentials) — DuckDB
  stands in via the subclassed IO manager. Control-M (no real connection possible or
  desired) — represented purely as an external `AssetSpec`, no execution attempted in
  either mode.
- **What can be real:** The NYC taxi seed data, deterministic and checked into
  `demo_data/`, and all DuckDB-backed materializations, checks, and aggregations —
  genuinely computed, not faked.
- **Asset kinds to display:** `bigquery` on the taxi pipeline. No `kafka`/`fivetran`/
  etc. — no ingestion tool is confirmed. Leave the coexistence vignette's external
  asset kind-free or labeled generically; there's no registry-standard "Control-M"
  badge.
- **Everything materializes green.** No planted anomaly. The three checks above are
  built, wired, and pass — the talk track is what they'd catch in production, not a
  staged failure.
- **Data realism notes:** small (~2–5k row) deterministic NYC taxi sample, seeded once
  and reproducible across runs and across a fresh clone. Row counts must not drift.

## Three buckets

- **In code:** the 7-asset graph above, 3 checks, 1 freshness policy, automation
  conditions, 1 schedule, the BigQuery IO manager subclass, the Control-M coexistence
  `AssetSpec` + observation sensor.
- **Dagster+ (demonstrate, don't build):** restart-from-failure (directly answers
  "wasted engineering time re-running whole pipelines instead of just the failed
  step" — show a single-partition retry, never build a custom retry mechanism);
  native alerting to Slack/Teams/email (answers the late-morning-batch SLA-miss pain —
  do not build custom alerting); asset health / lineage UI (answers "no end-to-end
  visibility" and "downstream teams lack self-serve visibility" directly — this is
  arguably the single most relevant Dagster+ capability to this prospect's stated
  pain, make sure it's on screen); run history and audit trail (answers governance
  concerns behind the "shadow stack" fear).
- **Conversation only:** the Bilt/Verifone customer-identity layer (unrelated to
  orchestration, do not build); Norm™ (their own AI agent, not ours); RunMyJobs/SAP
  specifically (mentioned as used at "other portfolio brands," not this meeting's
  focus — the Control-M vignette carries the coexistence point, a second legacy
  scheduler adds nothing); dbt/dlt/Informatica (undecided tools — mention that Dagster
  integrates with whichever they pick, build none of them).

## Demo name

**"Build It Twice"** — by hand, then by prompting Claude. The name should show up in
the README and DEMO_SCRIPT; it's also literally what the room is there to watch.

## Money shot

Two, because the ask has two halves:

1. The manually-built graph (`stg_taxi_trips` → `daily_zone_revenue` →
   `taxi_revenue_report`) and the Claude/MCP-built graph converge on the identical
   asset structure — reset to the starter checkpoint, prompt Claude in natural
   language with no ticket, and the same three assets appear, materialize, and pass
   the same checks in a couple of minutes instead of a manual walkthrough.
2. Click `inventory_snapshot_bigquery`, show `integration_pattern: coexistence` and
   `legacy_system_boundary` in the metadata panel, and say the line this whole
   vignette exists for: "your 3,000 Control-M jobs don't have to move on day one."

## Capability talk track

- **Asset checks firing on real data** → ties directly to "a job can show green while
  delivering stale or wrong-row-count data." Show the check results in the UI, not
  just their existence.
- **Restart-from-failure (Dagster+, not built)** → answers "wasted engineering time
  re-running whole pipelines instead of just the failed step" — this is the single
  highest-leverage Dagster+ capability for this prospect's stated pain; don't
  undersell it by rushing past it.
- **Freshness policy on `taxi_revenue_report`** → answers "morning batch jobs
  sometimes run late... no predictive ETA capability today."
- **Automation condition (eager, gated on the blocking checks)** → answers "wasted
  engineering time and compute re-running whole pipelines" from the automation side —
  only what actually needs to recompute does.
- **Lineage/asset health UI** → the direct answer to "no end-to-end visibility" and
  "downstream teams lack self-serve visibility" — let this breathe on screen; it's
  the single closest match to their stated pain of anything in this demo.
- **Dagster+ native alerting (not built)** → answers the SLA-miss escalation pain
  Suhail described (Slack/Teams/email/PagerDuty, whichever they'd actually use).

## Explicitly out of scope

- Any real BigQuery connection or credentials.
- A dbt project (transformation tool not chosen yet).
- Any dlt/Informatica ingestion integration (ingestion tool not chosen yet).
- A RunMyJobs/SAP component or vignette — Control-M alone carries the coexistence
  point for this meeting.
- Any Bilt/Verifone/customer-identity build — unrelated to orchestration, conversation
  only if it comes up.
- A full historical NYC taxi dataset — a small deterministic sample is sufficient and
  keeps `dg dev` fast on a shared screen.
- Building "Norm" or any agent of our own — the AI-driven half of the demo is Eric
  prompting Claude Code live against this project, not a shipped feature of the repo.
