---
company: "RVU (Tempcover)"
slug: "rvu-tempcover"
domain: "rvu.co.uk"
demo_date: "2026-09-04"
demo_time: "8:30–10:00 AM (calendar's raw UTC-4 offset; UI labels the zone America/Chicago, which doesn't match — likely a connector display quirk). That offset works out to roughly 1:30–3:00 PM UK time for the RVU/Tempcover attendees."
attendees: ["Lisa Smith — Data Engineering Manager, RVU/Tempcover, built the current Azure platform, business-context holder — lisa.smith@rvu.co.uk", "Iain Millar — Staff Data Engineer (~1 month in), technical champion, prior Dagster knowledge — iain.millar@rvu.co.uk (identity not independently confirmed publicly, see Conflicts)", "Samuel Garcia — Dagster AE (organizer) — samuel.garcia@dagsterlabs.com", "Eric Thomas — Dagster SE — eric.thomas@dagsterlabs.com", "sam@prefect.io and eric@prefect.io also on the invite — internal colleagues, role on this call unclear"]
ae_doc: "https://docs.google.com/document/d/1Ojtz-7Ayuw0jBqo-KNPzq-ug8O6qzObXKPTAD_M63Ik/edit"
ae_doc_modified: "2026-09-02"
overall_confidence: "high"
generated: "2026-09-02"
---

# RVU (Tempcover) — demo brief

## Demo thesis

Iain is one month into leading a from-scratch data-platform rebuild that is
**not optional** — legacy platform deprecation is forcing it — and two of the
three big decisions are already locked: BigQuery and dbt Core. Orchestration
is the one open decision, and Iain is already advocating internally for
Dagster over GCP's own Cloud Composer, unprompted, because he finds Airflow
"over-engineered and generic." Tom, the VP Engineering who isn't even on this
call, is waiting on a cost number before anything moves forward. So this demo
has exactly one job: turn Iain's already-favorable instinct into a concrete
technical argument he can hand to Tom. That means proving, specifically, that
Dagster replaces RVU's hand-built warehouse logging with real per-asset
observability (data was good, not just "the job ran"), that a pipeline
failure is fixed and rerun from the exact point it broke instead of the whole
job being disassembled and rerun, that dbt Core surfaces as first-class
assets and checks with no extra plumbing, and that BigQuery branch
deployments behave sanely against the one real BigQuery instance they have —
that last one is the specific mechanical question a technical champion this
deep into an evaluation will actually push on, and it's on his own list.

## Meeting

- **When:** 2026-09-04, 8:30–10:00 AM per the calendar's literal offset
  (~1:30–3:00 PM UK time for the RVU side) — 90 minutes, longer than a
  typical first-look demo.
- **Who's in the room:** Lisa Smith (Data Engineering Manager, built the
  current stack, the "business context" half of the room) and Iain Millar
  (Staff Data Engineer, the technical champion actively pushing for
  Dagster). Tom (VP Engineering, driving the budget conversation) is
  explicitly **not** on this call, per both the AE notes and the actual
  attendee list — he's a separate stakeholder to satisfy indirectly, via a
  cost estimate, not directly, via this demo.
- **Meeting type:** Technical deep-dive / competitive evaluation
  (Dagster+ vs. Cloud Composer, with self-hosted-on-GKE also on the table).
  90 minutes of runway means this can go deep rather than staying at
  survey-level.

## Use case — confidence: high

UK insurtech (RVU, parent of the Tempcover brand) rebuilding its data
platform from scratch. Lisa built the current Azure-based platform; Iain
joined a month ago to lead modernization. BigQuery and dbt Core are decided;
orchestration is the open decision, and RVU's small data engineering team is
already drawn to managed Dagster+ over self-hosting. (AE notes, direct quote
basis.)

## Current stack — confidence: high

| Layer | Tool | Source |
|---|---|---|
| Orchestration | None dedicated today (Azure Data Factory doing double duty); deciding between Dagster+ (managed) and Cloud Composer / self-hosted Airflow, with self-hosted Dagster on GKE also under consideration | AE notes — direct |
| Warehouse | Azure-based today (product unnamed); **BigQuery decided** for target | AE notes — direct for target |
| Transformation | Unnamed/ad hoc today; **dbt Core decided** for target | AE notes — direct for target |
| Ingestion | **Fivetran**, today and target | AE notes + corroborated in RVU job postings |
| BI | **Power BI**, today and near-term target | AE notes — direct |
| Activation | **Braze** (new, target) | AE notes — direct |
| Cloud | Azure today; GCP (BigQuery) target | AE notes — direct |
| CI/CD | **GitHub**, wants CI/CD tied to merges | AE notes — direct |

**Public cross-check (job postings, high confidence):** RVU's own postings
for Staff/Senior Data Engineer roles (London, Cardiff, Fleet, and one
specifically on "Tempcover's Data & Analytics team") list **BigQuery, dbt,
Airflow, Python, SQL**, and separately **Google Cloud Dataflow, Azure Data
Factory, Fivetran, and Databricks** as the tools in play. This confirms
every tool the AE notes name, and it also confirms **Airflow is a real,
named skill they're hiring for** — the "Dagster vs. Cloud Composer" framing
in the AE notes isn't a strawman, it's a live internal option. Databricks
and Dataflow appear in postings but not in the AE notes for this specific
rebuild — don't assume they're in scope for this project.

## Pain — confidence: high

Direct from AE notes, pain → impact:

- **Bespoke logging code** built just to track what ran when → engineering
  time spent on plumbing instead of building, "directly slowing the data
  delivery backlog that Lisa flagged as a key business problem."
- **Azure Data Factory has no asset-level observability** → "No way to know
  if data output was good, only whether the job ran" → business teams
  receive data they can't trust.
- **Monolithic DAG-based orchestration, full reruns on failure** → "Any
  pipeline failure requires disassembling, fixing, and rerunning the entire
  job" → longer MTTR, wasted compute, "directly contradicting Lisa's speed
  goal."
- **No lineage or dependency visibility** → when something breaks, unclear
  what else is affected → engineering spends heavy resources diagnosing
  instead of building.
- **No dev/prod separation or structured testing** → can't test safely
  before production → every deploy carries regression risk, which "directly
  blocks" Iain's stated goal of moving quickly with confidence.
- **Platform not AI-ready** → current Azure stack can't support a dbt
  Semantic Layer or structured LLM data access → blocks the business's AI
  ambitions (natural-language analytics, dashboard generation).

## Data domains

Not named explicitly in the AE notes (no specific table names given).
**Inferred from Tempcover's actual business** (confirmed via public
research): temporary/short-term motor insurance — quote requests, bound
policies, and feeds from a panel of insurers and 85+ corporate
partners/brokers. Mark this as an assumption, not a confirmed fact from the
room.

**Scale/cadence:** No concrete volumes or SLAs in the AE notes. RVU's
broader engineering org ships "up to 1,000 releases/week" platform-wide
(a velocity stat, not a data-volume one — don't conflate the two). Default
to daily partitioning as a safe, generic cadence.

## Public signals

- **Job postings:** Unusually strong signal here — **five concurrent open
  data/platform engineering roles** across RVU brands (Senior Data Engineer
  x2, Staff Data Engineer x2 including one specifically for Tempcover, and a
  Senior Platform Engineer), all in London/Cardiff/Fleet. This is a company
  actively and visibly investing in the exact rebuild the AE notes describe,
  not a speculative one-off project.
- **Engineering blog / GitHub:** None found.
- **Recent news:** RVU was founded in 2018 specifically to integrate five
  comparison brands (Uswitch, Confused.com, money.co.uk, Tempcover —
  acquired 2022 for >5x return to its prior PE owner, and Mojo Mortgages)
  onto one shared data platform. CTO Paul (ex-ThoughtWorks, at Uswitch since
  2010) is publicly framed as driving platform velocity ("up to 1,000
  releases/week") and an AI-productivity push — a well-resourced, technically
  serious platform org, not a shoestring team.
- **Industry / compliance:** Confused.com is FCA-authorised and regulated
  (#310635); UK GDPR applies across all brands given the personal financial
  data they process. Both are genuine constraints on the "how you'd trust
  this data" story, not decorative.
- **Orchestration signals:** Airflow/Cloud Composer is a real, named,
  actively-hired-for skill at RVU (see job postings above) — this is a
  genuine two-horse technical evaluation, not a formality. Iain's
  unprompted "not a fan of Airflow, finds it over-engineered and generic"
  is a strong technical-champion argument, and the AE notes flag it
  explicitly as reusable when Tom reviews the proposal.

## Conflicts and gaps

- **Iain Millar's identity could not be independently corroborated.**
  Public search turned up several different people named Iain Millar (at
  Octopus Investments, UBS, University of Hull) with no clear match to RVU
  or Tempcover. Treat his role/background as AE-notes-only, not
  independently verified.
- **Lisa Smith is corroborated** — a LinkedIn profile matching "Lisa Smith,
  Tempcover, based in Fleet" who has presented at RVU Connect events lines
  up with the AE notes.
- **A separate, seemingly unrelated "VP of Engineering (London)" posting**
  exists publicly for RVU's **Mojo Mortgages** brand specifically. Don't
  assume this is "Tom" from the AE notes — Tom's exact scope (Tempcover-only
  vs. RVU-wide) and surname are not given, and conflating him with the Mojo
  Mortgages VP Engineering role would be a confident-wrong-guess.
- **"Legacy platform deprecation forcing the rebuild now" has no public
  corroboration** — reasonable, since that's an internal timeline detail,
  but it's AE-notes-only.
- **Which entity is actually being demoed to is slightly ambiguous** — the
  AE notes say "UK-based insurtech (Tempcover/RVU)," attendee emails are
  @rvu.co.uk (the parent), and the Tempcover-specific job posting reports
  into "RVU's Data & Analytics team." Treat this as one shared,
  RVU-Group-owned data platform team whose immediate project is Tempcover —
  not two separate stakeholders.
- **Job postings additionally name Databricks and Google Cloud Dataflow**
  as ETL tooling — broader than what the AE notes describe for this
  specific rebuild. Supplementary context, not part of this build.

---

# Build directives

## Asset graph

Focused, ~16 assets, single-domain (Tempcover's temporary-insurance
business) rather than all five RVU brands — the AE notes scope this to one
rebuild, not a platform-wide migration, so don't inflate it:

- **`ingestion`** (via `dagster-fivetran`, matching their real, named
  ingestion tool): `raw_quote_requests`, `raw_bound_policies`,
  `raw_panel_insurer_feed`, `raw_partner_broker_feed` (the last two map
  directly to Tempcover's real panel-of-insurers / 85+ corporate-partner
  business model).
- **`dbt`** (a real, small dbt Core project — staging then marts, native
  `dagster-dbt`): `stg_quote_requests`, `stg_bound_policies`,
  `stg_panel_feed` → `fct_quotes_daily`, `fct_bound_policies_daily`,
  `dim_partner`. dbt's own tests (not-null/unique/relationships) surface as
  Dagster asset checks automatically — this is the single most direct
  answer to "dbt Core integration depth, auto-surfacing models and tests as
  asset checks," which is explicitly on their list.
- **`activation`**: `braze_customer_segment_export`, downstream of
  `fct_quotes_daily` — answers "Braze and data activation as observable
  assets" directly.
- **`reporting`**: `power_bi_quote_performance_report`, downstream of
  `fct_bound_policies_daily` — search the registry for a Power BI component
  first (see below) before building a plain AssetSpec.

**Partitions:** daily time partitioning on the two fact tables — no
volume/SLA numbers exist to justify anything fancier, and no second
dimension (region, brand, etc.) is named in the AE notes, so don't invent
one.

## Fidelity

**Graph-first**, per house default — but note dbt SQL runs for real
regardless of fidelity choice, and that's exactly where this prospect's
explicit ask ("dbt Core integration depth") lives, so graph-first still
delivers what they asked to see. Non-dbt assets (Fivetran ingestion, Braze
export, Power BI report) are demo-mode-mocked at the network boundary, per
`templates/demo_mode_pattern.py` — no real Fivetran/Braze/Power BI/BigQuery
credentials exist. Engine is DuckDB, badged `kinds={"bigquery"}` to match
their real target warehouse.

## Demo shape

**Build-a-pipeline.** This is a from-scratch rebuild, not an
orchestrate-existing-workloads or migration-both-states story — there's no
legacy orchestrator to observe (Azure Data Factory isn't being kept, it's
being replaced) and no second state to show side by side.

## Native integrations to use

- **`dagster-fivetran`** — named directly in both the AE notes and RVU's own
  job postings.
- **`dagster-dbt`** — dbt Core is a locked decision, not a maybe.
- **BigQuery** — `dagster-gcp`/BigQuery resource for the warehouse layer,
  `kinds={"bigquery"}`.
- **No Snowflake, no Redshift, no Databricks** — not this project's stack
  per the AE notes, even though Databricks appears in a job posting for a
  different scope.

## Community components to search for

- `power bi` (rung 2/3 candidate for `power_bi_quote_performance_report` —
  check for a `PowerBIWorkspaceComponent`-style integration before building
  a plain AssetSpec)
- `braze` (long shot — search before assuming nothing exists; if nothing
  turns up, that's a `component-feedback/` entry)
- `fivetran` / `dbt` (native integrations should cover these; search anyway
  per the escalation ladder)

## Asset checks

At least three, mapped directly to named pain — plus dbt's own tests count:

1. **dbt-native tests** on `stg_bound_policies` (not-null/unique on
   policy id) — auto-surfaced as Dagster asset checks. Directly answers "no
   way to know if data output was good, only whether the job ran."
2. **Blocking** — `raw_panel_insurer_feed` completeness check (row count
   floor) — blocks `fct_quotes_daily` from computing on an incomplete panel
   feed. Maps to "business teams receive data they cannot trust."
3. **Warning** — reconciliation check on `fct_bound_policies_daily` vs.
   `raw_bound_policies` row counts. Same pain, softer signal.

## Demo mode

- **What must be mocked:** Fivetran connector syncs, the Braze publish
  step, the Power BI refresh/query, and BigQuery itself — no real RVU/
  Tempcover credentials or systems exist.
- **What can be real:** DuckDB standing in for BigQuery; dbt Core running
  for real, with real tests, against DuckDB.
- **Asset kinds to display:** `bigquery` on the warehouse/dbt layer,
  `fivetran` on ingestion, and whatever kind matches Power BI/Braze if a
  registry component provides one — don't badge DuckDB.
- **Everything materializes green.** No planted failures. The "what happens
  when a check fails" story is entirely in the talk track below, not staged
  live.
- **Data realism notes:** Not applicable — graph-first, no synthetic data
  generation beyond what dbt's seed/fixture data needs to run its tests.

## Three buckets

- **In code:** The asset graph above (Fivetran ingestion, real dbt Core
  project with tests-as-checks, Braze and Power BI downstream assets),
  daily partitions, freshness policy on `fct_quotes_daily`, eager automation
  conditions on the dbt layer so new Fivetran syncs trigger rebuilds.
- **Dagster+:** Branch deployments against BigQuery (the literal mechanical
  question Iain asked — "how do ephemeral environments work when there's
  only one real BigQuery instance" — demonstrate/describe the
  schema-per-branch pattern live, don't fake it in code), restart /
  rerun-from-point-of-failure (the direct answer to their #1 named pain —
  "any failure requires rerunning the entire job"), run history and logging
  (replaces their bespoke warehouse logging code), native alerting
  (Slack/email/PagerDuty), RBAC and viewer licenses for business
  stakeholders, an insights/observability-over-time dashboard, and an
  admin-level log view. Don't hand-roll any of these.
- **Conversation only:** dbt Semantic Layer / structured LLM data access —
  a dbt-product and AI-roadmap topic, not something Dagster builds; mention
  it enables the AI ambitions once the platform is modernized, build
  nothing. Self-hosted-on-GKE as an alternative to Dagster+ — describe the
  Hybrid deployment option as the middle ground, don't build a self-hosted
  demo.

## Demo name

**"From Ran to Right"** — the whole pitch in four words: Azure Data Factory
can tell you a job ran; this shows whether the data was right.

## Money shot

Screen: the full lineage graph, green, Fivetran-sourced raw feeds flowing
into dbt staging and marts, then out to Braze and Power BI — one continuous
picture where today they have Azure Data Factory logs and nothing else.
Click `fct_bound_policies_daily`: show its freshness policy, its
dbt-test-derived passing checks, and row-count metadata in the same panel.
Then the talk track: "If this panel-feed check had failed, only this one
materialization would be blocked — not the whole job — and Dagster+ shows
you exactly which asset and why, then reruns from that exact point." That
sentence directly reverses Iain's own words: "any pipeline failure requires
disassembling, fixing, and rerunning the entire job." Close by opening a
second, empty branch deployment against the same BigQuery project and
walking through how it gets its own schema — answering his specific
"only one real BigQuery instance" question live rather than describing it.

## Capability talk track

Map every item on their own "what to see" list so nothing gets missed:

- **Run history / logging (Dagster+):** replaces the bespoke warehouse
  logging code that's currently eating engineering time.
- **Branch deployments with BigQuery (Dagster+):** answered live, per Money
  shot above — this is Iain's own named mechanical question.
- **dbt Core integration depth (built, native):** real dbt project, real
  tests surfaced as checks — no separate testing framework to maintain.
- **Asset catalog, freshness policies, checks (built):** direct answer to
  "no way to know if data output was good."
- **Lineage graph with upstream/downstream impact (built, native):** direct
  answer to "no lineage or dependency visibility."
- **Alerting via Slack/email/PagerDuty (Dagster+, not built):** don't
  hand-roll — show the native policy configuration screen.
- **Fivetran integration as assets (built, native):** ingestion is
  first-class in the graph, not a black box upstream of it.
- **Braze and data activation as observable assets (built):** sits in the
  same lineage graph as everything else, not a side integration.
- **RBAC and viewer licenses (Dagster+, not built):** how Tom's business
  stakeholders get visibility without edit access.
- **Insights dashboard for observability over time (Dagster+, not built).**
- **Admin-level log view (Dagster+, not built).**
- **Dev/prod separation and safe testing (Dagster+ branch deployments,
  covered above):** direct answer to "changes cannot be tested safely
  before hitting production."

## Explicitly out of scope

- No dbt Semantic Layer or LLM/AI data-access build — conversation only,
  dbt's own roadmap, not Dagster's to build.
- No self-hosted-on-GKE deployment — mention Dagster+ Hybrid as the
  alternative to full SaaS, don't build a self-hosted comparison.
- No Databricks or Google Cloud Dataflow assets — named in job postings for
  other roles, not confirmed as part of this specific rebuild.
- No planted failures or anomalies — the graph stays green; the "what
  happens on failure" story is entirely verbal, reversing Iain's own
  words about full-job reruns.
- No assets for RVU's other four brands (Uswitch, Confused.com,
  money.co.uk, Mojo Mortgages) — this rebuild is scoped to Tempcover per
  the AE notes; don't inflate it into a platform-wide RVU story.
- No real Fivetran, Braze, Power BI, or BigQuery credentials — demo-mode
  mocked throughout, per house rules on zero-setup.
