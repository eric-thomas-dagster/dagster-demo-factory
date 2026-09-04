---
company: "E.ON Sverige"
slug: "eon-sverige"
domain: "eon.se"
demo_date: "2026-09-17"
demo_time: "10:00-11:00 America/New_York (16:00-17:00 CET)"
attendees:
  - "Zulqarnain Mehdi — title unknown — zulqarnain.mehdi@eon.se"
  - "Thorbjörn Eriksson — title unknown — thorbjorn.eriksson@eon.se (also invited as thorbjorn.eriksson.external@eon.se — possibly a duplicate/contractor alias for the same person)"
  - "Tobias Dannerstedt — title unknown, .external address — tobias.dannerstedt.external@eon.se"
  - "Jesper Mortensen Blomquist — title unknown — jesper.mortensen_blomquist@eon.se"
  - "Sébastien Godet — title unknown — sebastien.godet@eon.se"
  - "Monica Kiefer — title unknown, eon.com (group-level?) domain — monica.kiefer@eon.com"
  - "Christer Friberg — title unknown — christer.friberg@eon.se"
  - "Martina Thorsell — title unknown — martina.thorsell@eon.se"
  - "Colin Beagan — Dagster Labs AE, organizer — colin@dagsterlabs.com"
  - "Eric Thomas — Dagster Labs, presenter — eric.thomas@dagsterlabs.com"
ae_doc: "none"
ae_doc_modified: ""
overall_confidence: "low"
generated: "2026-09-04"
---

# E.ON Sverige — demo brief

## Demo thesis

E.ON Energidistribution is mid-way through its largest-ever Swedish grid
investment (23-27 billion SEK, 2024-2027) and a nationwide smart-meter
replacement rolling out ~1M new NB-IoT-connected meters — both of which mean a
step-change in the volume and criticality of grid and metering telemetry
flowing through whatever E.ON currently orchestrates. At the same time, an EU
implementing regulation (2026/855, in force since April 2026) imposes
transparent, auditable data-access procedures for customer-switching data.
This demo has to prove that Dagster can ingest and validate that new
telemetry volume with asset-level lineage and checks that double as the audit
trail the new EU rule demands — a credible story even though we don't yet
know what E.ON runs today. **Confidence on all specifics below is low**: no
AE discovery notes exist, and public research could not confirm E.ON's actual
orchestrator, warehouse, or BI stack. This brief is built from job-posting and
company-investment signal only; treat every technical specific as directional,
not confirmed, and lean generic rather than guessing a vendor.

## Meeting

- **When:** 2026-09-17, 10:00-11:00 America/New_York (16:00-17:00 CET,
  Stockholm time)
- **Who's in the room:** Nine E.ON attendees, titles not exposed by the
  calendar invite. Domain mix is notable: most are `@eon.se` (the Swedish
  entity the AE credit calculator was built for), one is `@eon.com` (E.ON's
  group-level domain — possibly a corporate/shared-services stakeholder
  rather than someone local to Sweden), and two carry a `.external` suffix
  (`tobias.dannerstedt.external@eon.se`, and a second address for Thorbjörn
  Eriksson — likely a contractor or an external-tenant guest account for the
  same person invited twice). Nine attendees for a first "Demo/Deeper Dive"
  is a large room — treat this as cross-team, not a single data engineer's
  first look.
- **Meeting type:** Calendar labels it "Dagster Labs Demo/Deeper Dive with
  Colin" — a first demo / technical deep-dive per the invite title, not a
  recurring sync (no `recurringEventId` on this event) and not a POC
  check-in.

## Use case — confidence: low

No discovery notes exist to say what E.ON is explicitly trying to build. The
inferred use case, from public investment and regulatory signal only, is:
ingesting and validating grid/smart-meter telemetry at much higher volume as
the meter rollout lands, plus producing an auditable data trail for
customer-switching data under the new EU interoperability rule. This is an
inference from company-level news, not a stated requirement — say so if asked
in the room.

## Current stack — confidence: low

| Layer | Tool | Source |
|---|---|---|
| Orchestration | Unknown. Internal AE credit-calculator questionnaire classifies it as "Other" (i.e., not Airflow, not Dagster OSS, not dbt Cloud, not "Airflow + dbt Core") | AE credit calculator, 2026-09-01 |
| Warehouse | Unknown | — |
| Transformation | Unknown | — |
| Ingestion | Unknown — smart-meter telemetry arrives via Landis+Gyr's Gridstream Connect platform (NB-IoT/M1) at the network level; whether/how that feeds a central data platform is not public | Power Technology / E.ON press |
| BI | Unknown | — |
| Cloud | Unknown. Not confirmed for E.ON specifically — Azure/Databricks are common among *other* Swedish data-engineering job postings we found, but that is not E.ON evidence and must not be treated as such | Web search (see Public signals) |
| CI/CD | Unknown | — |

**Do not badge assets with a specific vendor kind on a guess.** Per house
rules, an unconfirmed stack layer should read generic in the demo rather than
risk a confident-wrong guess in the room.

## Pain — confidence: low

No AE notes exist with the prospect's own words. Inferred pain, from the
public signals below: a large, sudden increase in telemetry volume and
criticality (grid investment + meter rollout) arriving into whatever
orchestration exists today, plus a new external compliance deadline
(EU 2026/855) that requires provable, auditable data lineage for
customer-switching data specifically. Both are inferences, not quotes.

## Data domains

- Grid/network telemetry and load data (E.ON Energidistribution)
- Smart-meter interval reads (~1M new NB-IoT meters being deployed; Sweden-wide
  replacement of 5.4M metering points is the broader national context)
- Customer-switching data (subject to EU Implementing Regulation 2026/855's
  interoperability and access-transparency requirements)

No volumes, cadences, or SLAs are confirmed — all of the above is domain
framing, not measured scale.

## Public signals

- **Job postings:** E.ON Kundsupport Sverige AB has open Data Engineer roles
  in Norrköping and Malmö per Indeed/Glassdoor/Built In listings; full job
  description content was not retrievable (careers.eon.com and builtin.com
  were both blocked to this session's web-fetch tooling), so no specific
  tools could be confirmed from the postings themselves — only that active
  data-engineering hiring is happening. E.ON Sverige also has a documented
  "Digital Operations" team (per The Org) responsible for digital solutions,
  operational excellence, and data engineering.
- **Engineering blog / GitHub:** None found. No public E.ON engineering blog
  or GitHub org surfaced in search.
- **Recent news:** E.ON Energidistribution is investing 23-27 billion SEK in
  the Swedish grid from 2024-2027 — the company's highest-ever investment
  level, explicitly framed by leadership as "building, developing, and
  digitizing the electricity grid." E.ON Sverige separately announced a
  ~1 million-meter smart-meter rollout on Landis+Gyr's Gridstream Connect
  platform (NB-IoT/M1), part of Sweden's nationwide replacement of all 5.4M
  metering points. At the E.ON Group level, 2025 results show €8.5B total
  capex (€7.0B in Energy Networks) with a planned €48B 2026-2030 investment
  program, described as "consistently digitalizing the energy system." No
  new VP Data / CDO hire was found for E.ON in 2026.
- **Industry / compliance:** EU Implementing Regulation 2026/855 (in force
  April 2026) sets interoperability requirements and non-discriminatory,
  transparent procedures for the data needed for customer switching —
  directly relevant to any energy retailer's customer-data pipelines. GDPR
  applies as standard for an EU-headquartered company handling household
  consumption data.
- **Orchestration signals:** None confirmed publicly. The one internal signal
  (AE credit calculator, see Current stack) puts current orchestration in an
  "Other" bucket rather than Airflow or dbt Cloud, but that's a coarse
  categorical guess by whoever filled in the calculator, not verified detail.

## Conflicts and gaps

- **No AE discovery notes exist.** Searched Drive by company name, domain
  (`eon.se`, `eon.com`), all nine attendee surnames, "Sverige", "eon.se" full
  text, and Gmail for the domain — the only artifacts found are the AE's
  credit-calculator spreadsheet (2026-09-01) and the bare calendar invite
  itself. This brief is built from public research plus that one internal
  sizing document. Say so plainly in the room if asked what we know about
  their environment.
- **Stack is entirely unconfirmed.** Job-posting detail that might have
  named real tools was inaccessible (careers.eon.com and builtin.com both
  blocked to this session's fetch tooling) — this is a research limitation,
  not evidence of anything about E.ON's actual stack.
- **The room composition is unusual and unexplained.** Nine external
  attendees, mixed `eon.se`/`eon.com` domains, two `.external`-suffixed
  addresses. This could mean a cross-business-unit or group-level evaluation,
  or could mean nothing more than a large invite list. No way to resolve this
  without asking live.
- **Credit calculator implies deal sizing already underway** (500 data
  products/day, 50 seats, ~365K year-2 steady-state credits estimated)
  despite no discovery notes existing — someone on the Dagster side has
  engaged enough to size this, but didn't write it down anywhere this search
  could find.

---

# Build directives

## Asset graph

Given the confirmed-only signal is grid investment + smart-meter rollout +
customer-switching compliance, build a **grid & metering data platform**
narrative, generically presented (no invented vendor names), sized modestly
given the low confidence — about 10-12 assets:

- **Ingestion (group: `metering_ingestion`)** — `raw_meter_reads` (partitioned
  daily x grid-region, `MultiPartitionsDefinition`, matching the multi-region
  nature of a national meter rollout), `raw_grid_load_telemetry` (daily
  partition)
- **Validation/cleaning (group: `grid_quality`)** — `validated_meter_reads`
  (downstream of `raw_meter_reads`), `grid_load_hourly` (aggregated from
  telemetry)
- **Compliance (group: `customer_switching_compliance`)** —
  `customer_switching_extract` (models the EU 2026/855 interoperability
  data-access requirement), `switching_data_audit_log` (asset-level metadata
  demonstrating the auditability the regulation demands)
- **Reporting (group: `grid_reporting`)** — `daily_grid_health_summary`,
  `regional_meter_coverage_report`

Name assets in plausible Swedish-utility vocabulary
(`raw_meter_reads`, `grid_load_hourly`, `customer_switching_extract`) rather
than generic staging-table names, but do not invent specific source-system
names (no fake "SAP IS-U" or "Landis+Gyr API" integration unless treated as
external/observed rather than a fabricated live call).

## Fidelity

**Stubbed (default).** No named integration exists to build for real, and
confidence on every specific is low — this is a graph-first, always-green
build (asset bodies `pass`, deterministic where any metadata numbers are
shown). Do not fabricate synthetic meter-reading values as if they were real;
if row counts appear, keep them clearly synthetic/round.

## Demo shape

**build-a-pipeline.** No migration story is confirmed (we don't know what,
if anything, E.ON is moving off), and no "orchestrate existing workloads"
integration is named. Keep it a straightforward ingestion → quality →
compliance → reporting pipeline built on Dagster's own capabilities.

## Native integrations to use

None confirmed. Do not add dbt, Snowflake, Fivetran, Databricks, or any other
named integration on a guess — none of these appeared as E.ON-specific
evidence, only as patterns from unrelated Swedish job postings. If the build
routine wants any transformation layer, use plain Dagster assets or
`dagster-duckdb` for storage, badged generically (no snowflake/databricks
kinds) since the warehouse layer is unconfirmed.

## Community components to search for

Given no integration surface is confirmed, no registry search is mandated by
this brief. If the build routine wants to explore anyway (purely to check
for a "genuinely generic utility/IoT telemetry" component before writing one
by hand), reasonable search terms: `iot`, `time series`, `sensor`,
`telemetry`. Do not build a bespoke component for this — plain partitioned
assets cover the story.

## Asset checks

At least three, each tied to a pain named above:

1. **`meter_reads_completeness_check`** (blocking) — every expected meter ID
   for the partition's region reports a reading; answers the "sudden volume
   increase from the meter rollout" pain by refusing to let downstream
   compute on an incomplete feed.
2. **`grid_load_range_check`** (blocking) — flags physically implausible load
   values; answers the "criticality of grid telemetry" pain.
3. **`switching_extract_audit_completeness_check`** (warning-only is
   acceptable here only if a blocking check already exists elsewhere in this
   list; prefer blocking) — verifies the customer-switching extract carries
   the audit metadata fields the EU regulation implies; directly answers the
   compliance pain.

## Demo mode

- **What must be mocked:** Everything — there is no named external system to
  integrate with. All "sources" are internal mock generators representing
  E.ON's own (unconfirmed) metering and grid telemetry systems.
- **What can be real:** DuckDB storage, local files, deterministic synthetic
  generators.
- **Asset kinds to display:** Keep generic. Do **not** badge `snowflake`,
  `databricks`, `azure`, or any other specific vendor kind — none is
  confirmed for E.ON. If the UI needs some kind badge for visual polish,
  use the most neutral option available rather than a specific guess.
- **Everything materializes green.** No planted failures. Checks, freshness,
  and automation are visible and green; the compliance/audit story is told
  through metadata and check results, not a staged break.
- **Data realism notes:** Keep row counts small, round, and clearly
  synthetic (e.g., a fixed number of grid regions, a fixed meter count per
  region) — this is a low-confidence build and should not pretend to
  E.ON-scale numbers (millions of meters) on screen.

## Three buckets

- **IN CODE:** the ingestion/quality/compliance/reporting asset graph above,
  the three checks, one freshness policy, one automation condition, one
  schedule.
- **DAGSTER+:** alerting (Slack/Teams/email) on check failures — demonstrate,
  don't build; restart-from-failure; lineage visualization; run history.
- **CONVERSATION ONLY:** any actual integration with Landis+Gyr's Gridstream
  Connect platform, any real SAP IS-U or grid-operations system, and the
  specifics of how EU 2026/855 compliance reporting would actually be
  delivered to a regulator — these are all things to *discuss*, not build,
  since nothing about E.ON's real systems is confirmed.

## Demo name

**"Grid at Scale"** — chosen to carry the investment/rollout narrative
without asserting knowledge of E.ON's actual architecture.

## Money shot

A green asset graph showing `raw_meter_reads` (partitioned by day x region)
flowing through a blocking completeness check into `grid_load_hourly`, with
`customer_switching_extract` sitting downstream carrying visible audit
metadata. Eric's talk track: "Your meter rollout means this ingestion layer
is about to see several times the volume it sees today. Here's how Dagster
would tell you, per region, the moment a batch of meters stops reporting —
before it becomes a grid-operations problem — and here's the audit trail this
same lineage graph gives you for the new EU switching-data rule, for free,
because it's the same asset-level metadata either way."

## Explicitly out of scope

- No named integration builds (no dbt, no Snowflake, no Databricks, no
  Fivetran) — none confirmed for E.ON.
- No attempt to model real Landis+Gyr/Gridstream Connect API shapes.
- No large (20+) asset graph — the low confidence on use case doesn't
  support sizing up, unlike a brief built from real discovery notes.
- No specific regulatory-reporting format for EU 2026/855 — modeled only as
  "an extract with audit metadata," not a real compliance deliverable.
