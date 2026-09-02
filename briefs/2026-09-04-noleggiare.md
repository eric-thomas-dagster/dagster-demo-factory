---
company: "Noleggiare (FTH Group)"
slug: "noleggiare"
domain: "noleggiare.it"
demo_date: "2026-09-04"
demo_time: "10:00–11:00 AM ET (4:00–5:00 PM CET)"
attendees: ["Raffaele Vumbaca — Responsabile Business Intelligence (Finance dept), Noleggiare, coordinates the shared two-company BI team — raffaele.vumbaca@noleggiare.it", "Thomas Remelli — role unconfirmed, Noleggiare BI team (likely) — thomas.remelli@noleggiare.it", "Mattia Fausti — role unconfirmed, Noleggiare BI team (likely) — mattia.fausti@noleggiare.it", "Tomasi Auto attendee (name garbled in calendar invite, unconfirmed identity/title) — aalmustfahussinadamkhatir@tomasiauto.com", "Colin — Dagster AE (organizer) — colin@dagsterlabs.com", "Eric Thomas — Dagster SE — eric.thomas@dagsterlabs.com"]
ae_doc: "https://docs.google.com/document/d/1zdLQ3zBcwg8C4XlUfdcamNnt0P3cIAB2ohjVJUV-ep4/edit"
ae_doc_modified: "2026-09-02"
overall_confidence: "medium"
generated: "2026-09-02"
---

# Noleggiare (FTH Group) — demo brief

## Demo thesis

This is not a cold pitch: Raffaele's five-person BI team has already run Dagster
Starter for three months, unprompted, and built a real database migration
workflow and an ML pipeline on it — this call is an SE architecture review for
an engaged, self-serve trial user deciding how to scale up, not a
qualification conversation. What has to land today is that Dagster is the
single orchestration layer for **both** companies in the group (Noleggiare and
Tomasi Auto), not just a nicer script runner: one asset graph, with lineage
and data-quality checks, sitting between their existing Postgres-based sources
and Qlik Cloud, catching bad data before it reaches BI, with ML and ETL work
coexisting in the same graph the way Raffaele has already started building.
It also has to answer two structural questions Raffaele is deciding right now
without stating them as explicit asks: how a two-company BI team keeps its
pipelines visibly separate but centrally governed (asset groups + Dagster+
RBAC), and that swapping Postgres for Snowflake later, for the higher-volume
company, is a resource/io_manager change — not an orchestration rewrite. Land
that and the natural next step is the Hybrid-on-AWS deployment conversation he
has already telegraphed wanting.

## Meeting

- **When:** 2026-09-04, 10:00–11:00 AM ET (4:00–5:00 PM CET), 1 hour.
- **Who's in the room:** Raffaele Vumbaca (BI Manager, Finance dept,
  Noleggiare — coordinates the shared BI team across both companies, the
  clear decision-maker in the room); two more Noleggiare BI-team attendees
  (Thomas Remelli, Mattia Fausti — titles unconfirmed, presumed BI
  developers/editors); one Tomasi Auto attendee (email is a garbled
  concatenation in the calendar invite — identity and title unconfirmed);
  Colin (Dagster AE, organizer); Eric Thomas (Dagster SE).
- **Meeting type:** Titled "Dagster Labs Demo/Deeper Dive with Colin" —
  treat as a **technical deep-dive / architecture recommendation**, not a
  first-look demo. Non-recurring, single occurrence. Raffaele has already
  explored Dagster hands-on and, per the AE notes, "mainly wants to see what
  the SE recommends" — so lead with an opinionated architecture, not a
  feature tour.

## Use case — confidence: high

Modernize the shared BI team's ETL/ELT orchestration, moving off legacy,
non-code-first tooling onto Dagster as the code-first platform, with a
long-term goal of migrating essentially all ETL/ELT procedures to Dagster.
(AE notes, direct.)

## Current stack — confidence: medium

| Layer | Tool | Source |
|---|---|---|
| Orchestration | Legacy ETL tooling (today); Dagster Starter/Serverless (piloting ~3 months) | AE notes — direct |
| Warehouse | PostgreSQL (primary today); Snowflake leading candidate for future, Redshift/Databricks also evaluated | AE notes — direct |
| Transformation | Unconfirmed — no dbt or other named transform tool | AE notes silent; no dbt evidence found publicly |
| Ingestion | Source DBs → legacy ETL tooling → PostgreSQL | AE notes — direct |
| BI | Qlik Cloud (AE notes) / Qlik Sense (job posting) | AE notes + Tomasi Auto job posting |
| Cloud | AWS (infrastructure + marketplace relationship today; Raffaele wants Dagster+ Hybrid on AWS) | AE notes — direct |
| CI/CD | Unknown | — |

**Conflict worth flagging:** the AE notes describe the legacy ETL layer as a
"homegrown solution," but a Tomasi Auto job posting for a "Database &
Business Intelligence Specialist" lists **Talend, Pentaho, and Azure Data
Factory** as required tools alongside Qlik Sense and data-governance
principles. Both may be true (homegrown scripts around a Talend/Pentaho/ADF
core), but don't assume the legacy layer is pure hand-rolled code — it may be
commercial ETL tools the demo should feel comparable to replacing.

## Pain — confidence: high

Direct from AE notes:
- Current ETL tooling is "not modern or code-first."
- "Difficult to leverage AI for development with existing tools" — Raffaele's
  stated *why now*.
- Wants to consolidate more workflows onto a common orchestration layer.
- "Two companies currently need to coordinate around a shared BI/data
  platform" — a structural, org-boundary pain, not just a technical one.
- Wants to gradually introduce data quality checks and alerting, "not just
  ETL execution" — this is explicitly the *next* thing on their roadmap after
  the DB-migration and ML pilots.

## Data domains

Not stated explicitly in the AE notes beyond "database migration workflow"
and "an ML algorithm" already built on Dagster Starter — no specific tables,
volumes, or entities named. **Inferred from the two companies' actual
businesses** (confirmed via public research, not AE notes): Noleggiare is a
short/medium/long-term vehicle rental company (fleet >10,000 vehicles, 65
locations across Italy); Tomasi Auto is a multi-brand car dealer (~19,000
vehicles sold/year, >€300M revenue). So the domain is almost certainly rental
bookings/contracts/fleet data on one side and dealer sales/inventory data on
the other, consolidating into shared Finance-owned BI (Raffaele's team sits
in Finance). **Mark this as an assumption, not a confirmed fact** — no
specific asset/table names exist in the source material.

**Scale (confidence: high, from Noleggiare's own Dagster+ credit-sizing
sheet, not the discovery doc):** their own credit calculator estimates ~260
assets materializing per day at steady state, a 1.5x multi-asset/dbt-style
scaling factor, and 5 seats needing action-access — consistent with "5 BI
developers/editors" in the AE notes. No SLA or freshness cadence numbers
found; default to daily.

## Public signals

- **Job postings:** One directly relevant posting — "Database & Business
  Intelligence Specialist" at Tomasi Auto, listing **SQL, Talend, Pentaho,
  Azure Data Factory, Qlik Sense, and Data Governance** as required skills.
  This is the single best confirmation of a real (not purely homegrown) ETL
  toolchain — see Conflicts above.
- **Engineering blog / GitHub:** None found. Not a software company; no
  public engineering presence expected or found.
- **Recent news:** Both companies sit under **FTH SpA (Famiglia Tomasi
  Holding)**, led by Franco Tomasi (CEO) and Giancarlo Tomasi (chairman).
  Tomasi Auto sold 19,071 vehicles in 2025 (+9% YoY, >€300M revenue).
  Noleggiare (est. 2006) has grown to a fleet of 10,000+ vehicles. The group
  also runs **TC8 Srl**, the exclusive Italian importer of the Forthing
  (China-based) car brand — a **third entity in the same holding not
  mentioned in the AE notes**, worth being aware of as a possible future BI
  stakeholder even though it's out of scope for this build.
- **Industry / compliance:** Italian car rental is a genuinely hot market —
  ANIASA reports the sector hit €17B turnover in 2025 (+11% YoY), with the
  rental channel's share of new vehicle registrations climbing to 33.4% in
  H1 2026. That growth is a plausible tailwind for "why now" — a fast-growing,
  increasingly complex fleet/dealer data estate under one BI team. GDPR
  applies (EU/Italy, customer + financial data); no HIPAA/PCI-specific signal
  found, though rental/lease financing likely touches payment data.
- **Orchestration signals:** Not a competitive displacement — they are
  **already a self-serve Dagster Starter/Serverless user**, three months in,
  with real workflows built. This is an expansion/architecture conversation,
  not a bake-off.

## Conflicts and gaps

- **"Homegrown" (AE notes) vs. Talend/Pentaho/Azure Data Factory (job
  posting)** — both may be true; don't assume the legacy layer is pure
  custom code. See Current stack.
- **Which company is "the higher-volume company"** slated for Snowflake is
  not named in the AE notes. Tomasi Auto (>€300M revenue, ~19K
  vehicles/year) is plausibly the higher-volume side vs. Noleggiare's rental
  fleet business, but this is an inference, not a confirmed fact — don't
  assert it as certain in the room.
- **Specific data domains, table names, and volumes are not in the AE
  notes** — everything under Data domains above beyond team size and credit
  estimate is inferred from the companies' public business descriptions.
  Use generic, industry-plausible asset names; do not invent a specific
  source-system product name.
- **Two of four prospect attendees have unconfirmed titles** (Thomas
  Remelli, Mattia Fausti), and the Tomasi Auto attendee's email is a garbled
  string that doesn't cleanly parse to a name — treat their identities as
  uncertain going into the room.
- **A third group entity (TC8/Forthing) exists** and isn't mentioned in the
  AE notes — worth being aware of, not worth building for.

---

# Build directives

## Asset graph

Medium-sized graph (~16–20 assets) — big enough to carry the "two companies,
one platform" story and the ML-coexistence ask, without ballooning past a
one-hour deep-dive. Structure as three asset groups plus a shared layer:

- **`noleggiare_rental_ops`** (ingestion, Noleggiare's fleet/rental side):
  `rental_bookings_raw`, `fleet_vehicles_raw`, `rental_contracts_raw`.
- **`tomasi_dealer_ops`** (ingestion, Tomasi Auto's dealer side):
  `vehicle_inventory_raw`, `dealer_sales_raw`, `service_orders_raw`.
- **`shared_finance_warehouse`** (curated, owned by Raffaele's cross-company
  BI team): `dim_vehicle`, `dim_customer`, `fact_rental_contract`,
  `fact_vehicle_sale`, and a **partitioned, cross-company**
  `fact_finance_consolidated_daily` — this is the asset that directly answers
  "how do we structure Dagster across two companies" with company as a real
  partition dimension, not just a tag.
- **`ml_workflows`** (one asset, sitting inline in the same graph as ETL, not
  off to the side): `fleet_residual_value_forecast` — depends on
  `fact_vehicle_sale` and `fact_rental_contract`. This directly answers "how
  can ML workflows coexist with ETL pipelines," which Raffaele named
  explicitly.
- **`bi_publish`**: `qlik_cloud_export`, downstream of
  `fact_finance_consolidated_daily`.
- **`future_state_snowflake`** (small, deliberately parallel group):
  `fact_finance_consolidated_daily_snowflake` — same logical asset,
  `kinds={"snowflake"}`, same deps, different resource. Exists purely to
  make "swap the warehouse, not the pipeline" a literal thing Raffaele can
  see in the graph, per the AE notes' explicit ask to "briefly show how a
  future Snowflake migration fits without redesigning orchestration."

**Partitions:** daily time partitioning on the ingestion and warehouse
layers (no real SLA/cadence numbers exist — default to daily). Use a
`MultiPartitionsDefinition` (date × company: `noleggiare` /
`tomasi_auto`) on `fact_finance_consolidated_daily` specifically — company is
a genuine second dimension of their domain, not a decorative partition, and
it's the asset that has to carry the "two companies, one platform, still
separable" story.

## Fidelity

**Graph-first.** Nothing in the AE notes asks for a real number on screen —
the explicit ask is lineage, checks, alerting, ML/ETL coexistence, and
warehouse portability, not a KPI that has to move. Asset bodies are `pass`
(including the ML asset and the Snowflake-variant asset); checks return
static passing results. This also sidesteps the fact that no real Noleggiare
data, volumes, or schemas exist anywhere in the source material.

## Demo shape

**Build-a-pipeline**, with a light **migration-both-states** flavor limited
to one group: the `future_state_snowflake` pair exists specifically to show
current (Postgres) and future (Snowflake) state side by side, per the AE
notes. This is not a full legacy-orchestrator migration story — there's no
Airflow/Fabric/Synapse system to observe here — so don't build
trigger-and-observe machinery; the "future state" story is entirely about
warehouse portability, not migrating off another orchestrator.

## Native integrations to use

- **PostgreSQL** — `dagster-postgres` (or a Postgres resource/io_manager) for
  the `shared_finance_warehouse` and `noleggiare_rental_ops` /
  `tomasi_dealer_ops` groups, badged `kinds={"postgres"}` to match their real
  stack (engine can be DuckDB locally per demo-mode rules; kind badge is what
  matters visually).
- **Snowflake** — `dagster-snowflake`, used only on the
  `future_state_snowflake` group, badged `kinds={"snowflake"}`.
- **No dbt.** Not named in the AE notes or evidenced publicly — do not add
  a transformation-layer tool that isn't confirmed.
- Qlik Cloud has no confirmed native Dagster integration — see registry
  search below; if nothing fits, model `qlik_cloud_export` as a plain
  `@asset` with a demo-mode-mocked "publish" call (write to `demo_data/`),
  following `templates/demo_mode_pattern.py`, real Qlik Cloud REST API call
  when `demo_mode: false`.

## Community components to search for

- `postgres` (rung 2/3 candidate for the warehouse/ingestion layer)
- `qlik` (long shot — search before assuming nothing exists; if it turns up
  nothing, that's a `component-feedback/` entry worth writing)
- `snowflake` (native integration should cover this; search anyway per the
  escalation ladder)

## Asset checks

At least three, mapped to Raffaele's own stated pain:

1. **Blocking** — `fact_rental_contract` completeness: fail if a contract
   row is missing `customer_id`, `vehicle_id`, or `company_id`. Maps
   directly to "wants to gradually introduce data quality checks... not just
   ETL execution."
2. **Blocking** — cross-company consistency check on `dim_vehicle`: no VIN
   should appear as active fleet inventory in `noleggiare_rental_ops` and
   active dealer inventory in `tomasi_dealer_ops` at the same time. Maps
   directly to "two companies need to coordinate around a shared BI/data
   platform" — this is the single check that most directly demonstrates why
   a shared, governed platform beats each company running its own scripts.
3. **Warning** — `fact_finance_consolidated_daily` volume-band check: row
   count within an expected range per partition, catching a source going
   quiet before it silently starves Qlik dashboards.

## Demo mode

- **What must be mocked:** All source systems (rental booking/fleet system,
  dealer inventory/sales system — no real Noleggiare/Tomasi Auto systems or
  credentials exist), and the Qlik Cloud publish step (no Qlik API
  credentials available).
- **What can be real:** DuckDB standing in for both Postgres and Snowflake
  engines locally; local files for any seed/reference data.
- **Asset kinds to display:** `postgres` on the current-state warehouse and
  ingestion assets, `snowflake` on the `future_state_snowflake` group. Do
  not badge a specific rental/dealer-system product — none is confirmed.
- **Everything materializes green.** No planted failures. All three checks
  above pass; the talk track (below) carries what happens when one fires for
  real.
- **Data realism notes:** Not applicable — graph-first, no synthetic data
  generation.

## Three buckets

- **In code:** The full asset graph above (ingestion, warehouse, ML,
  Snowflake-variant, Qlik publish), the three checks, freshness policy on
  `fact_finance_consolidated_daily`, eager automation conditions on the
  warehouse layer, the company×date multi-partition, asset groups per
  company.
- **Dagster+:** RBAC/permissions to keep the two companies' pipelines
  separately scoped under one deployment (directly answers Raffaele's
  structural question — demonstrate the concept live, build nothing), native
  alerting on check failures (Slack/Teams/email), restart-from-failure, run
  history, lineage visualization, asset health. Also: the Hybrid-on-AWS
  deployment/agent architecture Raffaele has already said he wants — describe
  and diagram it, do not attempt to actually deploy against their AWS.
- **Conversation only:** TC8/Forthing (the third FTH Group entity) — mention
  if it comes up, build nothing. A real production Postgres→Snowflake
  migration plan — the demo shows orchestration portability, not an actual
  migration runbook.

## Demo name

**"One BI Team, Two Companies"** — names Raffaele's actual situation
exactly: one coordinated BI function, two legal entities, one platform.

## Money shot

Screen: the full graph, green, with `noleggiare_rental_ops` and
`tomasi_dealer_ops` visually distinct groups feeding one shared
`shared_finance_warehouse`, and `fleet_residual_value_forecast` (the ML
asset) sitting inline in the same lineage rather than off in a side project.
Click `fact_finance_consolidated_daily`, show its company×date partition
grid, its passing blocking check, and its freshness policy — then say "this
is one asset, materialized per company, and Dagster+ RBAC is what keeps a
Tomasi Auto user from touching Noleggiare's partitions without needing two
separate projects." Then click over to `fact_finance_consolidated_daily_snowflake`
and say: "same lineage, same checks, different warehouse resource — this is
the whole Postgres-to-Snowflake conversation, and it's a config change, not
a rewrite."

## Capability talk track

- **Asset checks (built):** "This is the data-quality-and-alerting layer
  you told us was next on your list, after the migration and ML work you've
  already done yourselves."
- **Freshness policies (built):** "This is how Finance would know a feed
  went stale before a dashboard silently goes wrong."
- **Automation conditions (built):** "New source data flows through
  automatically — no cron job either team has to remember exists."
- **Multi-partition + asset groups (built):** direct answer to "how do we
  structure this across two companies."
- **RBAC / permissions (Dagster+, not built):** "This is how you'd scope
  access so each company's team sees and acts on their own pipelines, under
  one deployment you administer centrally."
- **Alerting, restart-from-failure, run history (Dagster+, not built):**
  platform capabilities, shown live in the UI if time allows — don't
  overclaim them as something we wrote for this demo.
- **Hybrid-on-AWS deployment (Dagster+, not built):** speaks directly to
  what Raffaele has already said he wants next — describe the agent
  architecture, point at docs, don't fake a deployment.

## Explicitly out of scope

- No dbt — not evidenced for this prospect, despite the job posting naming
  other ETL tools.
- No real Qlik Cloud, Snowflake, or AWS credentials — demo-mode mocked
  throughout, per house rules on zero-setup.
- No TC8/Forthing-specific assets — third group entity, not mentioned in AE
  notes, out of scope for this build.
- No planted failures or anomalies — the graph is green throughout; the
  talk track carries what happens in production.
- No specific rental/dealer source-system product names — none confirmed;
  generic, domain-plausible asset names only.
- No real volumes or SLAs beyond the ~260-assets/day steady-state estimate
  already used for partition/cadence sizing — don't invent more precision
  than that.
