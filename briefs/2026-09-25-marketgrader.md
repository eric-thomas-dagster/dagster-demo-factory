---
company: "MarketGrader.com"
slug: "marketgrader"
domain: "marketgrader.com"
demo_date: "2026-09-25"
demo_time: "11:30 AM–12:30 PM America/New_York. 60 minutes."
attendees: ["Jim Andrews (jandrews@marketgrader.com) — Director of Index Solutions; initiated the evaluation, leads the POC, technical champion", "Carlos Diez (diez@marketgrader.com) — Founder & CEO; final decision-maker (with team input), self-described non-technical", "Pablo Delgado (pdelgado@marketgrader.com) — Director of Software Engineering; silent on the discovery call, buy-in not yet won", "Juan Be / \"Juanbe\" (jbermudez@marketgrader.com) — Director of Digital Product, based in Spain; barely spoke on discovery call", "Sabrina Muller (smuller@marketgrader.com) — Director of Web Development, based in Orlando; didn't speak on discovery call", "Naomi Mastico — Dagster SE, organizer", "Eric Thomas — Dagster SE"]
ae_doc: "https://docs.google.com/document/d/1LhFi1eO9_I_9Tk1OhqOUcpPYO221qgseViIwCxwCFuA/edit"
ae_doc_modified: "2026-09-24"
overall_confidence: "high"
generated: "2026-09-24"
---

# MarketGrader.com — demo brief

**REBUILD NOTICE:** This replaces the 2026-09-21 brief, which was written with
zero AE discovery notes and flagged everything `unknown`/`low confidence`. An
AE discovery-call doc ("Marketgrader Demo Prep for Eric," sourced from a Gong
call) was shared the afternoon before the demo. This version is built from
that doc and supersedes the prior one's guesses wherever they conflict — the
old brief's public-research framing (40k-company universe, Barron's 400 as
their flagship licensed index) is retained where it isn't contradicted, since
it's still the best available context for *what MarketGrader is*, even though
the *why they're on this call* and *current stack* sections below are now
confirmed rather than inferred.

## Demo thesis

Jim Andrews (Director of Index Solutions) is running a real, single-vendor
evaluation — Dagster is the only tool on the table — to replace a serial
SQL Server/SSIS-via-SQL-Agent batch chain that silently fails and depends on
tribal knowledge, with asset-based pipelines on a target Iceberg/Polaris
lake. The team scores 40,000+ companies daily and licenses the resulting
indexes (Barron's 400 among them) to funds and institutions, so a silent
failure or a late price feed doesn't just break a job — it risks shipping a
wrong number into a real financial product. Carlos Diez (CEO, non-technical,
final decision-maker) is not worried about features; he's worried about
**implementation time and production downtime** during cutover. This demo has
to prove two things at once: (1) lineage, blocking checks, and alerting turn
today's silent-failure/tribal-knowledge problem into something anyone can see
and safely re-run, and (2) the migration itself can be phased and low-risk —
observe the existing SQL Agent/SSIS estate first, migrate one index
end-to-end, prove it, then continue — not a risky big-bang cutover. Winning
Pablo Delgado (engineering director, silent so far) and de-risking Carlos's
downtime fear matter as much as the technical story.

## Meeting

- **When:** 2026-09-25, 11:30 AM–12:30 PM America/New_York. 60 minutes —
  "Dagster Labs Demo/Deeper Dive with Naomi." Discovery call (Gong-recorded)
  already happened; this is the technical deep-dive that follows it.
- **Who's in the room:**
  - **Jim Andrews** — Director of Index Solutions. Technical champion, leads
    the POC, initiated the evaluation. Cares about the migration path and
    dependency management specifically — his discovery questions were about
    late-feed detection, lineage across the cutover, and who can rerun a
    failed calc when the one expert is out.
  - **Carlos Diez** — Founder/CEO. Final decision-maker. Self-described
    non-technical. His stated fear is implementation time and downtime, not
    features — the phased-cutover story is aimed squarely at him.
  - **Pablo Delgado** — Director of Software Engineering. Was silent on the
    discovery call; his team ("the tech team," per AE notes, along with Juan
    and Sabrina) does the actual work, so his buy-in is unwon and matters.
  - **Juan Be ("Juanbe")** — Director of Digital Product, Spain. Barely spoke.
  - **Sabrina Muller** — Director of Web Development, Orlando. Didn't speak.
- **Meeting type:** Technical deep-dive following a discovery call. Dagster is
  the *only* tool in evaluation — no parallel bake-off, so there's no
  feature-for-feature competitive framing needed. The real competitor is the
  status quo (keep running SQL Agent/SSIS) or the project stalling from lack
  of bandwidth.

## Use case — confidence: high

Modernize a legacy, serially-run MS SQL Server + SSIS batch chain (orchestrated
by SQL Agent) that computes daily company ratings and constructs index
constituents (Barron's 400 among them) for a ~40,000-company universe, onto
asset-based pipelines on a target Iceberg/Polaris data lake, with output
egressed to customers/licensees. AE notes state the ideal state directly:
*"Asset-based pipelines on an Iceberg/Polaris lake, ingest to customer egress,
with alerts & lineage."* The near-term ask is not "migrate everything" — it's
a scoped POC: **migrate one index end-to-end** while the rest of the SQL
Agent/SSIS estate keeps running, with a lineage view that spans both.

## Current stack — confidence: high

| Layer | Tool | Source |
|---|---|---|
| Orchestration (current) | SQL Agent running SSIS packages, serially | AE discovery notes |
| Orchestration (target) | Dagster | AE discovery notes — sole evaluated tool |
| Data lake (target) | Iceberg / Polaris (catalog) | AE discovery notes, direct quote |
| Transformation (current) | SSIS | AE discovery notes |
| Cloud (deployment target) | IBM Cloud, S3-compatible object storage — hybrid deployment being evaluated | AE discovery notes; confirm agent compatibility before the demo |
| Ingestion | Price feeds + corporate-action feeds (vendor unnamed) | AE discovery notes (discovery Q1) + prior public research |
| BI | Unknown | No signal in either source |
| CI/CD | Unknown | No signal in either source |

This supersedes the prior brief's fully-`unknown` stack table. The one thing
still genuinely unconfirmed is the specific price/corporate-action data
vendor(s) — keep that generic in the build.

## Pain — confidence: high

Direct from AE notes: **"MS SQL Server + SSIS run serially via SQL Agent;
silent failures, tribal knowledge, manual checks."** Three concrete shapes of
that pain, each tied to a discovery question Jim asked:

1. **Late/wrong data detection** — "When a price feed or corporate action is
   late, how do you find out today, and what does a late or wrong index cost
   you with clients?" Implication: today, they may not find out until a
   client does.
2. **Cutover risk / lineage across old and new** — "Which SQL Agent jobs have
   to keep running during the migration, and would seeing legacy and new
   pipelines in one lineage graph change how you plan the cutover?"
3. **Tribal knowledge / recovery** — "If one of your in-house experts is out,
   who can rerun a failed index calc? What would it mean if anyone could see
   the failure and rerun just that step?"

Carlos's specific fear, separate from the technical pain above: **implementation
time and production downtime** during the cutover itself. That's a process/risk
concern, not a features concern, and the demo has to speak to it directly (see
Demo shape and Money shot below).

## Data domains

Fundamental/financial data and price/market data plus corporate-action events,
across a ~40,000-company universe (per prior public research on MarketGrader's
scoring business — not contradicted by AE notes). Cadence: daily scoring;
index constituents recompute periodically off of it. The near-term POC scope
is **one index, end-to-end** (Jim's proposed scope), not the full universe —
size the demo's synthetic data to a small, legible subset and say so
explicitly, matching the real POC's own scoping.

## Public signals

(Retained from the prior brief — not contradicted by AE notes, and still the
only source for MarketGrader's broader business context.)

- **Job postings:** None found for MarketGrader specifically as of the last
  search (2026-09-21). Company is small (~11–15 employees per LeadIQ).
- **Engineering blog / GitHub:** None found under the MarketGrader name.
- **Recent news:** Nothing dated 2026 surfaced in the prior search. MarketGrader
  Capital's most recent Form ADV (filed 2026-03-16) lists AUM as "None" —
  flagged, not interpreted, in the prior brief; still unresolved.
- **Industry / compliance:** MarketGrader Capital is a Florida-registered
  investment adviser — standard SEC/RIA recordkeeping expectations likely
  apply to anything feeding published index constituents. AE notes add a
  concrete data point here: the team is evaluating **RBAC and audit trail**
  specifically "for finance," and asking whether the deal justifies Dagster+
  **Pro rather than Starter** for a team this size — both point at real
  compliance/governance requirements, not just technical interest.
- **Orchestration signals:** AE notes now confirm SQL Server/SSIS/SQL Agent
  directly — this fully replaces the prior brief's "no signal found" note.

## Conflicts and gaps

- **The prior brief's entire stack table was `unknown`; AE notes now confirm
  SQL Server/SSIS/SQL Agent → Iceberg/Polaris.** No contradiction — this is
  new information filling a prior gap, not a conflict. Trust the AE notes.
- **Prefect merger question — flag for Eric personally, do not resolve in the
  brief or the demo build.** AE notes: *"Prefect came up only as a question
  about the merger... Jim's question went unanswered. Get the official answer
  on Prefect access and Dagster's roadmap before the demo so it doesn't become
  a lingering doubt."* This is a company/competitive question that needs an
  answer from Eric directly before walking into the room — it is explicitly
  **not** something this brief or the build routine can resolve, and nothing
  about it should be built into the demo. Surfaced again in the notification.
- **MarketGrader Capital's Form ADV shows AUM as "None"** despite actively
  licensing indexes to funds — still unresolved from the prior brief, still
  worth a mental note but not a build input.
- **Owed follow-ups from the discovery call** (not build items, but Eric
  should know they're outstanding): fin-services case studies (check logo
  rights), a one-pager, Forrester TEI study, scheduler/trial link, and the
  Prefect answer above.

---

# Build directives

## Asset graph

**Migration-both-states (coexistence, Pattern 4) — small and focused, sized to
the POC's own stated scope of "one index end-to-end."** ~8 assets:

- **`legacy_estate` group** (external assets only — Dagster observes, never
  triggers; SQL Agent stays master for these):
  - `legacy_sql_agent_price_ingest` — the still-running SQL Agent/SSIS job
    that ingests prices/corporate actions for every *other* index not yet
    migrated.
  - `legacy_sql_agent_index_calc` — the still-running legacy index-calculation
    job for those other indexes.
  - Both are `AssetSpec`s with no Dagster-owned compute, `integration_pattern:
    "coexistence"`, and `legacy_system_boundary` metadata making the cutover
    line explicit in the graph — directly answering Jim's discovery question
    about seeing legacy and new pipelines in one lineage view.
- **`lakehouse_ingestion` group** (new, Dagster-owned, target architecture):
  - `raw_price_feed` — daily-partitioned, Iceberg-badged (badge with whatever
    `kinds` term the Dagster UI actually supports for Iceberg/lakehouse tech;
    fall back to generic if none exists — verify at build time, don't guess).
  - `raw_corporate_actions` — daily-partitioned, same badge.
- **`index_calculation` group**:
  - `barrons_400_constituents` — the one real, publicly-named MarketGrader
    index (from prior public research), standing in for "the one index we
    migrate end-to-end" in the POC scope. Downstream of both raw assets.
    Eager automation condition — recomputes when its inputs land, not on a
    fixed schedule (this is a direct build of AE notes' stated flow: *"It
    runs when its inputs are ready, not on a timed chain"*).
- **`customer_egress` group**:
  - `licensee_distribution_extract` — the feed that goes to fund
    sponsors/institutions licensing the index. Downstream of
    `barrons_400_constituents`.
- **`reporting` group**:
  - `daily_coverage_summary` — rollup a research/ops persona would check
    (how much of the scoped universe scored cleanly today).

Drop the prior brief's speculative `sentiment_signals` asset — it was padding
under low confidence; the real POC scope (one index, end-to-end) doesn't need
it and adding it now would unfocus the money-shot story.

## Fidelity

**Stubbed (default), graph-first — same call as before, now for a better
reason: the POC's own stated scope is "one index end-to-end," which is a
lineage/cutover story, not a numbers-matter story.** Asset bodies are `pass`
except where real I/O is needed for the blocking checks to have something to
evaluate (deterministic synthetic fixtures for `raw_price_feed` /
`raw_corporate_actions`, per `templates/demo_mode_pattern.py`).

## Demo shape

**Migration-both-states / coexistence (Pattern 4), not build-a-pipeline.**
This is the single biggest change from the prior brief, which assumed a
from-scratch build because nothing was confirmed. AE notes are explicit that
the demo's coexistence beat is *"the cutover story for Carlos"* — show the
still-running SQL Agent/SSIS estate and the new Iceberg-based pipeline in one
lineage graph, with the legacy boundary clearly labeled. This is also
Carlos's demo, not just Jim's: the phased-cutover framing is what's supposed
to de-risk his downtime fear.

## Native integrations to use

- **SQL Server / SQL Agent legacy estate:** represent as external `AssetSpec`s
  per house rules for coexistence demos — do not build real SQL Server
  connectivity for the legacy side; it's observed, not executed, and
  demo_mode never needs real legacy credentials.
- **Iceberg / Polaris (target lake):** search the registry (see below) before
  assuming nothing fits — Axis 1 requires a real component here if one exists,
  even in early/thin form; subclass per rung 3 if it's close but not perfect.
  If genuinely nothing in the registry touches Iceberg/Polaris, that's a
  `component-feedback/` entry, not a license to build a bespoke "Iceberg
  component" that's really just wrapping a DuckDB or Parquet write — badge it
  honestly if it ends up generic.

## Community components to search for

- `iceberg`
- `polaris catalog`
- `pyiceberg`
- `sql server` / `mssql`
- `ssis`
- `sql agent`

Run all of these with `--json` before falling through to rung 4, per house
rules. `rvu-tempcover` and `stellantis-financial-services` both already solved
the "legacy scheduler stays master, Dagster observes" half of this pattern —
reuse that shape rather than re-deriving it.

## Asset checks

1. **`raw_price_feed` arrival/completeness — blocking.** Directly answers
   Jim's discovery question #1 ("when a price feed... is late, how do you
   find out today"). Downstream `barrons_400_constituents` refuses to compute
   on a missing/incomplete feed.
2. **`raw_corporate_actions` arrival/completeness — blocking.** Same reasoning,
   corporate-action side of the same discovery question.
3. **`barrons_400_constituents` day-over-day membership-change bounds —
   warning.** Retained from the prior brief: catches a scoring anomaly
   (e.g. a bug suddenly flipping hundreds of constituents) without blocking a
   legitimate rebalance.

## Demo mode

- **What must be mocked:** the price/corporate-action feed itself (vendor
  unnamed, no credentials exist) and the legacy SQL Agent/SSIS jobs (external
  assets, never executed for real).
- **What can be real:** DuckDB standing in for the Iceberg lake's I/O boundary
  (per `templates/demo_mode_pattern.py`); all Dagster-native capabilities
  (checks, freshness, automation, the observation sensor) run for real against
  synthetic fixtures.
- **Asset kinds to display:** `mssql`/`ssis`-adjacent badge on the two legacy
  external assets (match their actual current stack); an Iceberg/lakehouse
  badge on the two new raw assets if the Dagster UI has one — verify at build
  time rather than assuming, and use a generic badge instead of guessing.
- **Everything materializes green — this overrides part of the AE's own
  suggested demo flow, and that override belongs in the notification.** The
  AE notes literally suggest step 3 of the demo be *"a missing price file
  fails an asset check and fires an alert. Then rerun only the affected
  downstream assets"* — a live planted failure. **House rules are explicit
  that demos never plant a failure, with no exception a brief can grant.**
  Build the exact same checks, alerting-policy pointer, and
  restart-from-failure capability the AE notes describe — but deliver the
  "missing price file" scenario as a **talk track against a green graph**,
  not a staged live failure. Say this explicitly to Eric in the notification
  so it doesn't surprise him mid-demo-prep.
- **Data realism notes:** small, legible synthetic universe (not 40,000
  companies) matching the POC's own "one index" scope — say so in the demo
  script so nobody mistakes it for a claim about real scale.

## Three buckets

- **Implemented in code:** the 8 assets above; 3 asset checks; 1 freshness
  policy on `barrons_400_constituents` (directly answers "you'd know within
  minutes if today's ratings didn't update"); eager automation on the
  calculation → egress chain, gated on the blocking checks; a polling
  observation sensor over the legacy SQL Agent estate (per house rules'
  "observe, don't just execute" — someone kicking off a legacy job by hand
  should still show up); asset metadata including `integration_pattern` and
  `legacy_system_boundary`.
- **Handled by Dagster+:** alerting on check/freshness failure (demonstrate
  the policy UI live — do **not** build custom alert-firing code, even though
  AE notes describe "fires an alert" as part of the flow); restart-from-
  failure (directly answers Jim's discovery question #3 about the in-house
  expert being out); RBAC and audit trail (Carlos/finance's stated interest,
  and the Pro-vs-Starter tier question); branch deployments; asset catalog;
  run history/duration trends; hybrid deployment topology for IBM Cloud
  (point at the concept, do not attempt to actually deploy against IBM Cloud
  for this demo).
- **Conversation only:** the specific price/corporate-action data vendor
  (unnamed); real IBM Cloud hybrid-agent wiring; infosec kickoff and POC
  scope/timeline negotiation; the Prefect merger question (Eric's to answer
  personally, not a build or demo item — see Conflicts and gaps above).

## Demo name

**"One Index, End to End"** — names the actual POC scope Jim proposed (migrate
one index fully, prove it, then continue) rather than overclaiming a
full-platform migration.

## Money shot

Materialize the graph green, then click into `barrons_400_constituents` and
walk the lineage in one view: the still-running `legacy_sql_agent_*` assets
sitting alongside the new `raw_price_feed` → `raw_corporate_actions` →
`barrons_400_constituents` → `licensee_distribution_extract` chain, with the
legacy boundary clearly labeled. Say directly to Carlos: *"this is what a
phased cutover looks like — the indexes you haven't migrated yet keep running
exactly as they do today, fully visible here, while this one index is now
fully on the new platform. Nothing about migrating index two requires you to
touch index one."* That's the downtime/risk answer he needs, built directly
into the same graph that gives Jim his lineage and checks story.

## Capability talk track

- **Blocking checks on `raw_price_feed` / `raw_corporate_actions`:** "a late or
  garbled price feed or corporate action stops here, before it reaches a
  published, licensed index constituent list — you find out here, not from a
  client asking why the index looks wrong." (Answers discovery Q1 directly.)
- **Freshness policy on `barrons_400_constituents`:** "you'd know within
  minutes if today's index didn't recompute, not when a licensee asks."
- **Eager automation:** "the index recomputes the moment its inputs land —
  not on a fixed schedule, and not something someone has to remember to
  kick off."
- **Observation sensor on the legacy SQL Agent estate:** "even the indexes you
  haven't migrated yet show up here — if someone kicks off that SQL Agent job
  by hand at 2am, Dagster sees it and records it. Lineage spans your whole
  estate during the cutover, not just the piece we've touched today."
- **Dagster+ alerting (platform, not built here):** point at the alerting
  policy UI for the checks above — "this is where a check failure pages
  whoever owns data quality, no custom code, and it's the same mechanism the
  missing-price-file scenario in your notes would trigger."
- **Restart-from-failure (platform, not built here):** directly answers
  discovery Q3 — "if the person who normally reruns a failed index calc is
  out, anyone with access sees exactly which asset failed and reruns just
  that one step. No tribal knowledge required."
- **RBAC and audit trail (platform, Pro tier):** point at it for Carlos and
  the finance/compliance angle AE notes raised — a real answer to the
  Pro-vs-Starter question, not a built feature.
- **Branch deployments / catalog (platform, Pro tier):** brief mention per the
  AE's own suggested flow step 5, no build required.

## Explicitly out of scope

- **No planted failure, despite the AE notes' own suggested flow** — see
  Demo mode above. This is a hard house-rule override, and it's called out
  in the notification, not silently changed.
- No real price/corporate-action vendor integration — vendor unnamed, nothing
  to build against.
- No real IBM Cloud hybrid-agent deployment — conversation only.
- No attempt to answer the Prefect-merger question in the brief, the build, or
  the demo script — that's Eric's, before the room.
- No `sentiment_signals` asset (dropped from the prior brief — see Asset
  graph above).
- Index rebalance-cadence modeling beyond "recomputes when inputs are
  ready" — not needed for the one-index POC scope.
