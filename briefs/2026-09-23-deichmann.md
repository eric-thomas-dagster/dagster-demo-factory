---
company: "Deichmann SE"
slug: "deichmann"
domain: "deichmann.com"
demo_date: "TBD"        # no meeting booked yet — see Meeting section
demo_time: ""
attendees: []          # unconfirmed — see Meeting section
ae_doc: "https://docs.google.com/spreadsheets/d/1q8xDs-YRG0dlLhjPO5nWXi8s-v7ZpN-PNbL8AXa2JP8/edit — 'Deichmann <> Dagster - credit calculator'"
ae_doc_modified: "2026-09-23T15:11:48Z"
overall_confidence: "low"
generated: "2026-09-23"
---

# Deichmann SE — demo brief

## Demo thesis

Deichmann's Data Platform Engineering team runs roughly 450 Airflow DAGs a day
(≈11,250 task-runs/day, per their own sizing numbers) orchestrating a
multi-brand, multi-country retail group — Deichmann, Snipes, Ochsner
Sport/Shoes, Dosenbach, Van Haren, Rack Room Shoes — right as open-source
Airflow 2.x hits end-of-life in April 2026, forcing a real decision rather
than a hypothetical one. This demo has to prove that Dagster can absorb that
scale and structure — one estate, N brands, N countries — as a single legible
asset graph, not a bigger pile of DAGs, and that asset-level checks and
freshness give their Databricks lakehouse-fed customer/product "data
products" (the thing they're actively trying to activate operationally via
Lakebase for e-commerce personalization) the trust guarantees their current
setup can't. If they believe this, the pitch isn't "replace Airflow with
another scheduler" — it's "your lakehouse's data products become governed,
observable assets your marketing and personalization systems can depend on."

**Read this whole brief with real skepticism.** It is built almost entirely
from public research plus one internal pricing-sizing spreadsheet — there are
no AE discovery notes and no meeting is booked. See Conflicts and gaps.

## Meeting

- **When:** Not booked. No calendar event exists for Deichmann in the next
  14 days or the prior 6 weeks. This brief exists because an AE (Austin,
  `austin@dagsterlabs.com`) filled out Dagster's internal credit-sizing
  calculator with Deichmann-specific numbers and shared it today
  (2026-09-23) — that's an active-pipeline signal, not a scheduled demo.
- **Who's in the room:** Unconfirmed. Public org data names **Christian
  Schillinger, VP Data & Analytics** (joined 2024, ex-Henkel Corporate VP
  Data & Analytics, ex-QIAGEN) as the senior data executive, and **Kevin
  Haferkamp, Head of Data Platform & Engineering CoE, IT Data & Analytics**
  as the platform lead. Neither is confirmed as an attendee for any specific
  meeting — there isn't one yet.
- **Meeting type:** Unknown — likely an early technical evaluation given the
  sizing-calculator stage, but could equally be a first intro call once
  something is booked.

## Use case — confidence: low

Best public read: Deichmann's Data & Analytics unit runs a cloud-based
Azure + Databricks lakehouse and is trying to move governed data products
(customer, product) out of a small, hard-to-maintain set of "standardized"
extracts into broader operational use — specifically e-commerce marketing
and personalization — via Databricks Lakebase (a managed Postgres serving
layer in front of the lakehouse). Orchestration of that estate currently runs
on Airflow at real scale. Nothing in hand states they're formally evaluating
Dagster for a migration versus for augmentation (observe-and-govern
alongside Airflow) — the credit calculator's "current orchestrator: Airflow"
framing is consistent with either.

## Current stack — confidence: medium

| Layer | Tool | Source |
|---|---|---|
| Orchestration | Apache Airflow (currently) | AE credit calculator (high conf. on volume: 450 DAGs/day, ~25 tasks/DAG) + job postings |
| Warehouse / Lakehouse | Azure Databricks (lakehouse), Databricks Lakebase (managed Postgres serving layer) | Databricks customer case study, job postings |
| Transformation | dbt, Apache Spark, Python | Job postings ("Team Lead Data Platform Engineering", "Senior Data Engineer") |
| Ingestion | unknown | — |
| BI / activation | Databricks Lakebase → e-commerce marketing/personalization systems (not a traditional BI tool) | Databricks case study |
| Cloud | Microsoft Azure (primary); job posting also asks for "ideally also GCP" experience | Job postings |
| CI/CD | Terraform, Azure ARM, Pulumi (IaC); CI/CD, automated testing, observability named as engineering standards | Job posting |
| E-commerce platform | Migrating all 41 online shops to SCAYLE (separate from the data platform) | scayle.com case study, just-style.com |

## Pain — confidence: low-medium

No AE-quoted pain exists. Inferred from public sources:

- Databricks' own case study describes Deichmann sharing only "a limited
  number of standardized data products... due to maintenance challenges,"
  and frames Lakebase as solving that — i.e., their current pipeline/
  governance model doesn't scale to more data products without more manual
  maintenance burden.
- Open-source Airflow 2.x reaches end-of-life April 2026, which is a forcing
  function on anyone still running it at Deichmann's stated scale
  (~11,250 task-runs/day).
- Nothing found (yet) that is a directly quoted complaint from a Deichmann
  employee. Treat "pain" here as informed inference, not fact.

## Data domains

Footwear retail across 7 brands (Deichmann, Dosenbach, Ochsner Sport, Ochsner
Shoes, Snipes, Solebox, Van Haren) plus Rack Room Shoes (US) — ~4,700 stores,
34 countries, ~€8.9B 2025 revenue, ~49,000 employees. Likely domains: store
and e-commerce sales/orders, inventory, customer/loyalty data, product
catalog, and the customer/product "data products" the Lakebase case study
names as feeding personalization and marketing. Volumes/cadence beyond the
Airflow DAG count are unknown.

## Public signals

- **Job postings:** At least 6 open data roles found on Deichmann's careers
  site as of this run: Team Lead Data Platform Engineering, Team Lead Data
  Engineering, Team Lead Data Science Engineering, (Senior) Data Engineer,
  (Senior) Data Scientist, (Senior) Data Scientist Customer Analytics, and a
  Senior ML Engineer/Data Scientist Customer Data role — a genuinely active
  hiring push into the Data & Analytics org. JDs name Python, Apache Spark,
  Apache Airflow, dbt, Azure, Azure Databricks, Terraform/Azure ARM/Pulumi.
- **Engineering blog / GitHub:** None found.
- **Recent news:** Deichmann opened a new headquarters and reported ~€8.9B
  2025 revenue (+2%+ constant currency); 2026 investment plans name the new
  campus, store modernization, and expanding digital/omnichannel capability.
  VP Data & Analytics Christian Schillinger joined in 2024 from Henkel —  not
  brand-new in 2026, but still the newest senior data leader on record, and
  consistent with a re-platforming budget existing. Separately, Deichmann is
  mid-migration of all 41 online shops to the SCAYLE commerce platform, and
  has digitalized procurement (Ivalua) — both signal a company actively
  investing in modern platforms group-wide, not just in data.
- **Industry / compliance:** EU-headquartered multinational retailer handling
  consumer data across many countries — GDPR applies; no retailer-specific
  compliance regime (PCI would apply to payment data, typically scoped away
  from an orchestration layer) surfaced beyond that.
- **Orchestration signals:** Current orchestrator is Airflow at real scale
  (per their own sizing inputs). Airflow 2.x is reaching end-of-life in
  April 2026, which is an industry-wide re-evaluation trigger this run leans
  on but cannot confirm is Deichmann's actual stated reason.

## Conflicts and gaps

- **No AE discovery notes exist.** Everything above comes from one internal
  pricing-sizing spreadsheet (Airflow volume numbers only) plus public
  research. Do not treat the use case, pain, or stack table as confirmed —
  treat them as this run's best inference, clearly marked low/medium
  confidence throughout.
- **No meeting is booked.** `demo_date` is `TBD`; this brief may sit unused
  for a while, or may need a fast rewrite once an actual meeting and
  discovery notes exist.
- **Migration vs. augmentation is unresolved.** The credit calculator's
  Airflow-as-current-orchestrator framing is consistent with a full
  Airflow-replacement pitch or an observe-and-govern-alongside-Airflow
  pitch. This brief specs the former (see Demo shape) as the more common
  reading of a credit-sizing exercise, but flags it as a real fork the AE
  should confirm before the room.
- **BI/activation layer is genuinely unknown.** Lakebase is a serving layer,
  not a BI tool — there's no confirmed dashboard/reporting tool to badge.

---

# Build directives

## Asset graph

Size: medium (~10-12 assets). This is a platform-consolidation story (many
brands/countries under one orchestration layer), which argues for breadth,
but confidence is low enough across the board that this run intentionally
does not try to enumerate 20+ brand-specific assets it cannot justify from
real signal — pick 3 illustrative brands, not all 7, to keep the graph honest
about what's confirmed vs. representative.

- **Group `raw_ingestion`** (kinds: `databricks`, `azure`) — one asset per
  brand/country pair, partitioned on `MultiPartitionsDefinition` of
  `date` × `brand_country` (e.g. `deichmann_de`, `snipes_us`,
  `ochsner_sport_ch`). Represents landing raw transactional/customer/product
  data into the Databricks lakehouse. Use a workspace-style component
  (Databricks) with an explicit brand/country → job-task mapping table, per
  the one-component-instance-many-objects rule — not one instance per brand.
- **Group `staging`** (kinds: `dbt`, `databricks`) — real dbt Core staging
  models per domain (customer, product, order), sourced from the raw layer.
- **Group `entity_data_products`** (kinds: `dbt`, `databricks`) — real dbt
  Core marts producing the "customer" and "product" data products the
  Lakebase case study names — `customer_360`, `product_catalog_enriched`.
  These are the assets a personalization/marketing system would actually
  consume.
- **Group `activation`** (kinds: `databricks`) — one asset representing the
  Lakebase-style operational serving extract (e.g.
  `customer_segments_activated`), downstream of `entity_data_products`, with
  a freshness policy — this is literally the capability Deichmann is already
  investing in per the Databricks case study, so it's the natural "why does
  this need to be fresh and governed" answer.

## Fidelity

**Stubbed (default).** Confidence on this brief is low enough, and the
scaling/structure story is the point, not specific numbers. Asset bodies in
`entity_data_products` and `activation` are `pass`; real dbt SQL still runs
for the dbt-owned layer per house rules (dbt is never mocked). Revisit to
data-backed only if a real meeting and real discovery notes raise the bar.

## Demo shape

**build-a-pipeline**, with an Airflow-replacement framing — not
orchestrate-existing-workloads and not migration-both-states, because
nothing confirms Deichmann wants their legacy Airflow DAGs represented
in-graph (that would require knowing what those DAGs actually do, which
isn't in hand). If a real meeting surfaces "we want Dagster alongside
Airflow, not instead of it," this build directive is wrong and should be
corrected in a real brief before build.

## Native integrations to use

- **`dagster-dbt`** — real dbt Core project for `staging` and
  `entity_data_products`. Confirmed via job postings (dbt named alongside
  Spark/Airflow/Python).
- **Databricks workspace component** — for `raw_ingestion`, badged
  `databricks`/`azure` kinds. Demo-mode subclass per
  `templates/demo_mode_pattern.py`, same shape as the Umicore build
  (`DemoDatabricksWorkspaceComponent`).
- **BI/ingestion tools:** unknown — do not invent one. No Fivetran,
  Snowflake, or specific BI tool is evidenced; leave those layers out
  entirely rather than guessing.

## Community components to search for

- `databricks workspace`
- `databricks job task`
- `dbt project`
- `azure databricks`

## Asset checks

At least three, each mapped to a pain above:

1. **Blocking** — completeness check on each brand/country's raw ingestion
   partition before staging computes. Answers: "the current pipeline model
   can't scale data products without more manual maintenance" — this is the
   Dagster answer to that.
2. **Freshness policy** — on `customer_segments_activated` (the Lakebase-style
   activation asset). Answers: "how would marketing/personalization know the
   data they're activating on is stale."
3. **Warning** — a schema/PII shape check on `customer_360` (GDPR-relevant
   fields present and typed correctly across country partitions). Answers
   the EU/multi-country compliance angle.

## Demo mode

- **What must be mocked:** Databricks workspace API (no real Databricks
  workspace/credentials for Deichmann). Subclass the workspace component per
  `templates/demo_mode_pattern.py`; discovery/list calls return a fixed
  brand/country list.
- **What can be real:** DuckDB standing in for the Databricks-backed tables;
  real dbt Core execution (DuckDB adapter) for staging/entity layers.
- **Asset kinds to display:** `databricks`, `azure`, `dbt` — never `duckdb`.
  The dbt translator needs overriding (`get_asset_spec` → `replace_attributes
  (kinds={"dbt", "databricks"})`) since the manifest's `adapter_type` will
  say duckdb by default.
- **Everything materializes green.** No planted anomaly. Checks and
  freshness are built, visible, and passing; Eric talks through what each
  one would catch in production.
- **Data realism notes:** 3 illustrative brand/country partitions
  (`deichmann_de`, `snipes_us`, `ochsner_sport_ch`), small deterministic row
  counts (dozens to low hundreds), seeded generators.

## Three buckets

- **IN CODE:** dbt-native asset checks + one custom completeness check +
  one PII/schema check, freshness policy on the activation asset, eager
  automation on the dbt layer gated on the blocking check, Databricks
  workspace component (demo-mode subclassed) with brand/country mapping,
  asset groups/kinds for visual fidelity.
- **DAGSTER+ (demonstrate, never build):** alerting to Slack/Teams/email on
  check failures or freshness breaches, restart-from-failure, lineage
  visualization across the raw→staging→entity→activation chain, asset
  health/landing page, RBAC (relevant given a multi-brand, multi-team org),
  branch deployments (relevant given the "multiple teams shipping DAG
  changes to one shared instance" pain that's endemic to Airflow at this
  scale).
- **CONVERSATION only:** the SCAYLE e-commerce platform migration itself
  (that's their commerce layer, unrelated to orchestration — mention as
  context for why they're investing broadly in modern platforms, build
  nothing), the Databricks Lakebase serving layer itself (Databricks' own
  capability — Dagster feeds it governed data products, doesn't reimplement
  it), Ivalua procurement digitalization (unrelated).

## Demo name

**"One Lakehouse, Every Brand"**

## Money shot

Rematerialize `snipes_us`'s raw ingestion partition for today's date. Watch
it cascade automatically (eager automation, gated on the blocking
completeness check) through `stg_customer`/`stg_product` and into
`customer_360`/`product_catalog_enriched`, down to
`customer_segments_activated` — all green, all in one asset graph spanning
three brands and three countries. Say: "This is what a 450-DAG, 11,000-task
Airflow estate looks like as one legible, checked, freshness-tracked graph —
per brand, per country, without a rewrite."

## Capability talk track

- **Asset checks (blocking + warning):** "Your Databricks case study says
  you're limited to a small number of standardized data products because of
  maintenance overhead. This is what removes that ceiling — a check gates
  bad data before it reaches a data product, so you can add more of them
  without adding more manual QA."
- **Freshness policy on the activation asset:** "This is the direct answer
  to 'how do we know the data feeding personalization is current' — no
  custom monitoring script, it's declared on the asset."
- **Declarative automation:** "Airflow schedules time; Dagster recomputes
  when the input actually changed. At your DAG volume, that's the
  difference between one scheduler per team improvising fixed times and one
  system reasoning about what actually needs to run."
- **Dagster+ RBAC / branch deployments (platform, not built):** "You have
  multiple teams (Team Lead Data Engineering, Team Lead Data Platform
  Engineering, Team Lead Data Science Engineering per your own org chart)
  shipping into one estate — this is what stops that from being one shared
  Airflow instance everyone's afraid to touch."
- **Dagster+ alerting (platform, not built):** "Every check and freshness
  policy here can page Slack/Teams/PagerDuty directly — nothing here is a
  custom notification system."

## Explicitly out of scope

- No attempt to represent all 7 brands or Rack Room Shoes (US) — 3
  illustrative brand/country pairs only, clearly labeled as representative.
- No BI/reporting asset — no BI tool is evidenced.
- No ingestion-tool integration (Fivetran/Airbyte/etc.) — none evidenced;
  raw ingestion assets represent Databricks-native landing only.
- No attempt to model the SCAYLE e-commerce migration or Ivalua procurement
  work in the asset graph — conversation-only context, not build targets.
- No Airflow-side assets or trigger/observe pattern — this brief specs a
  replacement story, not a coexistence one (see Conflicts and gaps on why
  that's a real fork, not a confirmed choice).
