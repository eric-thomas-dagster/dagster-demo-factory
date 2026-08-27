---
company: "City of Detroit — Water & Sewerage Department (DWSD)"
slug: "detroit-dwsd"
domain: "detroitmi.gov"
demo_date: "2026-08-28"
demo_time: "1:00 PM ET"
attendees: ["Anthony Urbina — Application Analyst II, DWSD Integrations — anthony.urbina@detroitmi.gov", "Austin — Dagster AE (organizer) — austin@dagsterlabs.com", "Eric Thomas — Dagster SE — eric.thomas@dagsterlabs.com"]
ae_doc: "none"
ae_doc_modified: ""
overall_confidence: "medium"
generated: "2026-08-27"
---

# City of Detroit — Water & Sewerage Department (DWSD) — demo brief

## Demo thesis

Anthony Urbina sits on DWSD's Integrations team, and DWSD has a live posting for
that exact team ("Applications Analyst 2 (Integrations)") asking for someone to
"design, develop, support and document data flows from line of business
applications... to DWSD's data warehouse" across "legacy, web, cloud, and
purchased package environments," hand-coded in T-SQL, PL/SQL, Java, and Python,
including "high frequency data loads." No orchestrator, lineage tool, or
observability layer is named anywhere in that posting — this reads as
point-to-point integration sprawl that someone (possibly Anthony himself) is
being hired to hand-maintain. This is a 30-minute first intro call, so the
thing it has to prove isn't a deep technical story — it's that Dagster is the
layer that turns "a pile of scripts moving water-utility data into a
warehouse" into a governed, observable, lineage-visible pipeline, without
requiring DWSD to rewrite the SQL Server / Oracle / packaged-app integrations
already in place. The concrete hook: when a resident's water bill is wrong or
an EPA compliance report is late, "which job touched this data and did it
pass its checks" is exactly the question DWSD currently has no fast answer to.

## Meeting

- **When:** 2026-08-28, 1:00–1:30 PM ET (30 min)
- **Who's in the room:** Anthony Urbina (Application Analyst II, DWSD
  Integrations — the likely hands-on builder of the exact integrations
  described below); Austin (Dagster AE, organizer); Eric Thomas (Dagster SE).
  No other DWSD or City of Detroit attendees on the invite.
- **Meeting type:** Intro / first call. Non-recurring, created 2026-08-24,
  titled "Intro with Dagster." Treat as an early qualification conversation,
  not a technical deep-dive — the demo should be legible in a first look, not
  dense.

## Use case — confidence: medium

DWSD needs to move data from multiple line-of-business systems (billing/CIS,
meter reading, work order/asset management, water-quality lab systems — exact
products unconfirmed) into a central DWSD data warehouse, reliably and on a
documented schedule, across a genuinely mixed legacy/cloud/packaged-app
environment. This is inferred entirely from the DWSD job posting language,
not from a stated project name or AE notes — there is no confirmation this is
an active initiative Anthony is personally driving versus backlog pain the
posting was written to solve.

## Current stack — confidence: medium

| Layer | Tool | Source |
|---|---|---|
| Orchestration | None named — likely hand-scheduled scripts/jobs | DWSD posting doesn't mention one; inferred gap |
| Warehouse | DWSD "data warehouse" (product unnamed) | DWSD posting, direct quote — high confidence it exists, low confidence on product |
| Transformation | T-SQL (SQL Server), PL/SQL (Oracle), Java, Python | DWSD posting, listed as required skills — high confidence |
| Ingestion | Custom point-to-point integrations, "legacy, web, cloud, and purchased package environments" | DWSD posting, direct quote |
| BI | Unknown | — |
| Cloud | "Multi-cloud, tenant environments" per posting — specific provider(s) unknown | DWSD posting |
| CI/CD | Unknown | — |

**Separately, elsewhere in the city (not DWSD, not this contact):** the City
of Detroit's own GitHub org (`CityOfDetroit`) maintains a fork of
`airflow-dbt-python` — direct evidence the city runs **Apache Airflow + dbt**
somewhere, almost certainly for the Department of Innovation & Technology's
(DoIT) long-running Socrata open-data portal (data.detroitmi.gov, live since
2015, 75+ datasets from 9 agencies). This is a different department than
DWSD and there's no evidence Anthony's team uses it — see Conflicts and gaps.

## Pain — confidence: medium

The DWSD posting's own language: engineers "design and model application
data structures, storage and integration... across legacy, web, and cloud and
purchased package environments," with "high frequency data loads" needing
optimization. Read plainly, that's a data engineer's job description with no
mention of an orchestrator, a lineage graph, or a testing/checks layer —
which for a public water utility means billing and compliance data quality
depends on undocumented tribal knowledge of which script ran in what order.
No AE notes exist to confirm this is a stated pain in Anthony's own words;
this is our inference from the job market, not a quote from the room.

## Data domains

Water/sewerage utility data, inferred from DWSD's function and the posting's
"high frequency data loads": customer billing and usage, meter reads (likely
AMI/smart meter feeds), work orders/asset maintenance, and water-quality
compliance readings (Safe Drinking Water Act reporting is a near-certainty
for any US water utility, though not confirmed for DWSD specifically). Scale,
volumes, and SLAs are unknown — no numbers found publicly or in AE notes.

## Public signals

- **Job postings:** Notably active hiring for data infrastructure across at
  least three separate City of Detroit departments right now: DWSD
  ("Applications Analyst 2 (Integrations)" — T-SQL/PL-SQL/Java/Python,
  multi-cloud data warehouse integrations), the Health Department ("Senior
  Health Data Analyst" — explicitly "collaboratively designing a
  department-wide data warehouse, data catalog, and data pipeline
  infrastructure" from scratch), and a citywide "Data Visualization &
  Reporting Analyst III" role using nearly identical "organization-wide data
  warehouse, data catalog, and data pipeline infrastructure" language. Three
  departments independently standing up similar foundational data
  infrastructure, under one Chief Data Officer, is itself a signal worth
  naming in the room if it comes up — but it's a citywide observation, not
  something to build for a DWSD-specific call.
- **Engineering blog / GitHub:** `github.com/CityOfDetroit` — 80 public repos,
  mostly civic-tech apps (rental registry map, affordable-housing map, a
  design system). The one data-infrastructure-relevant repo is the
  `airflow-dbt-python` fork noted above.
- **Recent news:** Kat Hartman has been the City's Chief Data Officer since
  February 2024 (previously Director of Data Strategy & Analytics since
  2020) — a real, tenured executive sponsor for data governance at the city
  level, even though she isn't on tomorrow's invite. Detroit also runs a
  USDOT-funded "SMART MODES" smart-intersection program (traffic/pedestrian
  safety sensor data, partners Derq/Miovision/UrbanLogiq) — a different,
  unrelated data domain, noted only for context.
- **Industry / compliance:** Public-sector data carries FOIA/open-records
  transparency obligations and, for the Police Department specifically, CJIS
  compliance and a 30-day ShotSpotter retention policy — none of that is
  DWSD's domain, but it signals the city takes data governance and retention
  seriously enough to have named policies. Water utilities are typically
  subject to EPA Safe Drinking Water Act compliance reporting; not confirmed
  for DWSD specifically.
- **Orchestration signals:** No orchestrator named in the DWSD posting at
  all — read as a gap, not a migration-away-from-X signal. The Airflow+dbt
  evidence lives in a different department (DoIT), so this is *not* an
  "Airflow migration" story for DWSD; it would be a ground-up orchestration
  story instead.

## Conflicts and gaps

- **No AE discovery notes exist for this prospect.** Everything above is
  built from a 14-day calendar scan and public research — job postings,
  GitHub, and open-data-portal history. Treat every "confidence: medium"
  label above as genuinely uncertain; nothing here has been confirmed by
  anyone at DWSD or the City of Detroit.
- **DWSD's stack vs. the city's Airflow+dbt evidence are two different
  departments.** Do not conflate them — there is no evidence DWSD uses or
  even knows about DoIT's Airflow+dbt fork. Building a demo that implies
  DWSD already runs Airflow would be a confident-wrong-guess.
- **Anthony Urbina's exact title and role scope are inferred from a
  RocketReach listing ("Application Analyst II, DWSD," pursuing an MS in
  Cybersecurity at University of Toledo), not from the calendar invite or an
  AE note.** Plausible and specific enough to build from, but unconfirmed.
- **Specific product names for DWSD's billing/meter/work-order/lab systems
  are unknown.** Do not name real vendor products (e.g., a specific CIS or
  CMMS brand) — use generic, industry-recognizable asset names instead.

---

# Build directives

## Asset graph

Focused, DWSD-specific graph — roughly 10–14 assets, sized to a single
first-call story rather than a citywide platform pitch (that's the Health/DoIT/
citywide angle, explicitly out of scope below). Group by source domain, all
feeding a central warehouse layer:

- **Ingestion group** (`dwsd_ingestion`): `billing_system_extract`,
  `meter_reading_extract` (AMI/high-frequency — this is the asset that
  earns a partition), `work_order_extract`, `water_quality_lab_extract`.
  Model these as the kind of point-to-point sources the job posting
  describes — "legacy, web, cloud, and purchased package environments."
- **Partitions:** daily partitions on `meter_reading_extract` and
  `water_quality_lab_extract` (the two domains where "high frequency data
  loads" and regulatory-cadence readings are named/implied in the posting).
  Daily is the safe default cadence since no real SLA number exists.
- **Warehouse/curated group** (`dwsd_warehouse`): `dim_customer_account`,
  `fact_meter_reads`, `fact_billing_usage`, `water_quality_compliance_daily`,
  `work_order_status`.
- **Reporting group** (`dwsd_reporting`): `billing_accuracy_report`,
  `compliance_reporting_extract` (the artifact that would go to a regulator).

## Fidelity

**Graph-first** (default). Nothing here needs a real number on screen for a
30-minute intro call — the story is lineage, checks, and orchestration across
a heterogeneous source landscape, not a specific row count. Asset bodies are
`pass`; no synthetic data generators, no demo_mode I/O apparatus. This also
sidesteps the total lack of real volume/cadence numbers noted above — nothing
to guess wrong.

## Demo shape

**Build-a-pipeline**, not orchestrate-existing or migration — there's no
confirmed existing orchestrator at DWSD to observe or migrate from (unlike
the DoIT Airflow+dbt situation, which is a different department and out of
scope here). This is a from-scratch "here's what governing your existing
integrations would look like" story.

## Native integrations to use

None confirmed for DWSD specifically — build with plain Dagster
`@asset`/`AssetSpec` definitions in YAML via `defs.yaml` where possible,
matching the graph-first, `pass`-bodied approach. **Do not add dbt** — dbt is
evidenced for a different department (DoIT), not DWSD, and adding it here
would be the confident-wrong-guess this file exists to prevent. If DWSD turns
out to already use dbt on a follow-up call, that's a fast, cheap addition
later.

## Community components to search for

Given the graph-first, `pass`-bodied approach and no confirmed external
system credentials or APIs, a plain asset/AssetSpec structure in `defs.yaml`
should cover this without needing a registry component. If time allows,
search anyway before assuming nothing fits:
- `sql server` / `mssql` (T-SQL is a named DWSD skill)
- `oracle` (PL/SQL is a named DWSD skill)
- `water utility` / `utility billing` (long shot, but check)

## Asset checks

At least three, mapped to the pain above:

1. **Blocking** — `meter_reading_extract` completeness check: fail if the
   partition's row count falls below an expected floor. Maps directly to
   "high frequency data loads... optimization" pain — a utility can't bill
   accurately on incomplete meter data.
2. **Blocking** — `water_quality_compliance_daily` completeness check: no
   missing required readings for the day. Maps to the (unconfirmed but
   industry-standard) EPA compliance-reporting pain named in Data domains.
3. **Warning** — `billing_accuracy_report` reconciliation check: flag
   accounts where billed usage diverges from raw meter delta beyond a
   threshold. Maps to "which job touched this and did it pass" — the
   demo-thesis question.

## Demo mode

- **What must be mocked:** Everything — there are no real DWSD credentials or
  APIs available, and no confirmed real system names to integrate against.
- **What can be real:** DuckDB as the local engine; local files for any
  seed/reference data.
- **Asset kinds to display:** `sqlserver` on the billing/CIS-flavored assets
  and `oracle` on anything modeled after the PL/SQL-flavored systems — both
  directly justified by named DWSD required skills. Leave cloud-provider
  kind off entirely rather than guess AWS/Azure/GCP — "multi-cloud" in the
  posting isn't specific enough to badge accurately.
- **Everything materializes green.** No planted failures. Checks are wired,
  passing, and visible; the talk track carries what happens when they fire
  in production.
- **Data realism notes:** Not applicable — graph-first, no synthetic data.

## Three buckets

- **In code:** The DWSD asset graph above, its checks, freshness policies,
  and automation conditions.
- **Dagster+:** Alerting on check failures (Slack/email/Teams — don't build
  a custom notifier), restart-from-failure, run history, lineage
  visualization, asset health.
- **Conversation only:** The citywide angle — DoIT's existing Airflow+dbt
  pipeline, the Health Department's from-scratch data warehouse build, and
  the fact that three departments are independently solving similar
  problems under one Chief Data Officer. Worth Eric raising verbally as "this
  looks bigger than DWSD alone" — build nothing for it. Also conversation
  only: CJIS/police data, ShotSpotter — unrelated domain, don't reference in
  the demo itself.

## Demo name

**"One Warehouse, Every Pipe"** — a water-utility-appropriate name for the
"many source systems, one governed warehouse, one lineage graph" story.

## Money shot

Screen: the full DWSD asset graph, green, lineage flowing from four
heterogeneous ingestion sources (badged `sqlserver`/`oracle` to match their
real stack) through a curated warehouse layer into a compliance report — one
graph, one place to look. Eric clicks `water_quality_compliance_daily`,
points at its passing blocking check and its freshness policy, and says: "If
this were late or incomplete, this would be red before it ever reached a
compliance report — and you'd see exactly which upstream extract caused it."
Then click `meter_reading_extract`'s metadata panel to show `owner`,
`sla`, and `business_impact` fields already filled in — the story that this
isn't a diagram, it's operable.

## Capability talk track

- **Asset checks (built):** "Here's what catches a bad meter or lab reading
  before it reaches billing or a compliance filing" — directly answers the
  posting's unstated pain.
- **Freshness policies (built):** "This is how you'd know a data-quality
  problem before a resident calls about their bill, not after."
- **Automation conditions (built):** "New extracts trigger the warehouse
  rebuild automatically — no cron job someone has to remember exists."
- **Lineage graph (built, native):** "Every one of these hand-written
  T-SQL/PL-SQL/Java integrations your posting describes becomes one node
  with visible upstream/downstream — the map that doesn't exist today."
- **Alerting, restart-from-failure, run history (Dagster+, not built):**
  "This is a platform capability, not something we wrote for this demo" —
  show it live in the UI if the call goes long enough, don't overclaim it as
  custom-built.

## Explicitly out of scope

- No dbt, no Airflow orchestration/observation — neither is confirmed at
  DWSD; both belong to a different department's evidence.
- No specific vendor product names for DWSD's real systems (CIS, CMMS, LIMS,
  etc.) — none are confirmed publicly.
- No police/CJIS/ShotSpotter data or naming — different department, and a
  genuinely sensitive/controversial public topic locally; irrelevant to
  DWSD's story and not worth the risk of looking like we researched
  something adjacent and uninvited.
- No citywide/Health-department build — that's a much bigger, separate demo
  if this call surfaces interest in it; don't gold-plate this one trying to
  cover it.
- No real volumes, SLAs, or cardinalities — none exist publicly; graph-first
  fidelity avoids needing them.
