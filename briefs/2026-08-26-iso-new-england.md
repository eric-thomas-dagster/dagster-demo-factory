---
company: "ISO New England"
slug: "iso-new-england"
domain: "iso-ne.com"
demo_date: "2026-08-26"
demo_time: "13:00 America/New_York"
attendees:
  - "Andrew Rogers — title unknown (technical stakeholder, arogers@iso-ne.com — see notes)"
  - "B. Tekin — title unknown (btekin@iso-ne.com)"
  - "Colin (Dagster Labs, Account Executive) — colin@dagsterlabs.com"
  - "Eric Thomas (Dagster Labs, Sales Engineer) — eric.thomas@dagsterlabs.com"
ae_doc: "https://docs.google.com/document/d/1M_uxO1EwooBDSeucXFdXJXZ3IVZXf-t7dJ1q-b7776c/edit"
ae_doc_modified: "2026-08-17T21:51:11Z"
overall_confidence: "high"
generated: "2026-08-24T21:00:00Z"
---

# ISO New England — demo brief

## Demo thesis

ISO New England's data team is not evaluating whether Dagster can orchestrate —
they're already running Dagster OSS against dbt/Oracle/Postgres and learning
sensors on their own. Their real question is why they'd ever need Dagster+.
This demo has to prove that OSS already fixes their stated pain (dumb
fixed-interval schedules, no selective materialization) and that Dagster+ is
the next step only once they need catalog, lineage, freshness, alerting,
RBAC, and governance at platform scale — the things a highly regulated,
security-review-heavy organization (NERC CIP, ongoing FERC compliance
projects) will eventually be asked to prove to auditors and stakeholders
anyway. If the demo sells "orchestration," it has already lost; it has to
sell "the platform layer above the orchestration you already trust."

## Meeting

- **When:** 2026-08-26, 1:00–1:45 PM ET (Zoom)
- **Who's in the room:**
  - Andrew Rogers (arogers@iso-ne.com) — the technical voice quoted in the AE's
    demo-prep notes; specifically wants "a communication mechanism for users
    around what is happening with the data platform." Title not confirmed by
    any source available to this run.
  - B. Tekin (btekin@iso-ne.com) — no additional context found in AE notes or
    public search; likely a data/platform engineering colleague of Andrew's.
- **Meeting type:** Technical deep-dive / continuation of an active technical
  evaluation. This is not a cold first demo — ISO-NE is already hands-on with
  Dagster OSS.

## Use case — confidence: high

Orchestrate a growing set of dbt pipelines as ISO-NE moves off a legacy
Oracle system and builds a more modern data platform. They are already
testing Dagster OSS and learning sensors on their own initiative. The ask for
this specific meeting is to understand the case for Dagster+ once OSS is
already solving their immediate orchestration problem.

## Current stack — confidence: high

| Layer | Tool | Source |
|---|---|---|
| Orchestration | Dagster OSS (already in use); legacy jobs run on fixed schedules (hourly / every 3 hours / daily) | AE notes |
| Warehouse | Oracle (legacy, being migrated off) → PostgreSQL (target) | AE notes; corroborated by a live "Data Engineering Manager" posting requiring Oracle + PostgreSQL fluency |
| Transformation | dbt | AE notes; corroborated by same job posting requiring dbt experience |
| Ingestion | Not specified | unknown |
| BI | Not specified — ISO-NE's live "Data Platform Manager" posting references an "enterprise data warehouse, data marts, and BI data systems," but no specific tool is named | job posting, medium |
| Cloud | Not confirmed for the data platform team specifically. Unrelated ISO-NE software-engineering postings mention AWS/Kubernetes/Java-Spring Boot, but that appears to be a different (non-data) engineering group | inferred, low |
| CI/CD | Git/PR workflow is "relevant" per AE notes — no further detail | AE notes, medium |

## Pain — confidence: high

Direct from the AE's demo-prep notes: *"Current scheduling is inefficient and
painful. They want to trigger work only when needed, selectively run relevant
assets, and eventually give users better visibility into data status."*

Specifics:
- Jobs run on "dumb schedules" (hourly / every-3-hours / daily) regardless of
  whether work is actually needed.
- They want sensors/event-driven execution to replace fixed schedules.
- Their dbt/Postgres environment has real operational complexity around
  **views vs. materialized tables** — they want to selectively materialize
  only what actually needs it.
- They are maintaining the legacy system while migrating off it, which caps
  how fast they can move.
- Longer-term: **"Andrew specifically called out needing a communication
  mechanism for users around what is happening with the data platform."**
  This is the closest thing to a named persona pain in the notes, and it maps
  directly onto freshness policies + a user-facing status view.
- **Important context from the AE:** highly regulated organization; security
  reviews, asset management, and setup can take months. Don't undersell how
  long procurement/security will take, and lean into governance/RBAC as a
  head start on that review rather than a hurdle.

## Data domains

The AE notes describe the *pipeline shape* (Oracle source → Postgres →
dbt → views/materialized tables → downstream users) but do not name the
underlying business data domain, and no discovery call transcript exists to
confirm one. Public context: ISO-NE administers a $9B+ wholesale electricity
market and real-time grid operations for ~15M people across six states — so
plausible domains are market/settlement data, generation or metering
telemetry, or transmission/system-planning data — but which of these (if any)
actually feeds *this* dbt project is **unknown**. The build should use
generic, clearly-utility-flavored asset names (e.g. metering/telemetry- or
market-operations-flavored) rather than inventing a specific named business
entity the AE notes never mentioned.

Volumes, SLAs, and exact cadence beyond "hourly / every 3 hours / daily" are
not specified — **unknown**.

## Public signals

- **Job postings:** Two live, relevant postings as of this run: a **Data
  Platform Manager** (owns the enterprise data warehouse, data marts, and BI
  systems) and a **Data Engineering Manager** (8+ years, SQL/Python/dbt,
  Oracle and PostgreSQL). Both corroborate the AE notes' stack and confirm
  there's real headcount/budget behind the "Unified Data Platform" push, not
  just one engineer's side project.
- **Engineering blog / GitHub:** None found. ISO-NE has no public engineering
  blog or GitHub org.
- **Recent news:** ISO-NE's 2026 Annual Work Plan lists capacity-market
  reform, new reserve products, and long-term transmission planning as
  top initiatives, and explicitly states the ISO is "developing software and
  systems" to keep up with FERC compliance obligations — a real
  platform-modernization tailwind, not just a data-team initiative. Dr. Vamsi
  Chadalavada became President/CEO in January 2026 after 18 years as EVP/COO —
  an internal promotion rather than an outside re-platforming hire, so treat
  it as context, not a budget signal.
- **Industry / compliance:** NERC CIP is the operative compliance regime
  here (not HIPAA/PCI/SOC2). CIP-003-9 enforcement began April 1, 2026
  (governance for low-impact BES cyber systems, vendor remote access/supply
  chain), and CIP-012-2 takes effect July 1, 2026 (protecting real-time
  operational data). This is exactly the "security reviews... can take
  months" context the AE flagged — RBAC, audit logs, and governance framing
  should be pitched as helping clear that bar, not just as nice-to-haves.
- **Orchestration signals:** They are not evaluating Dagster cold — they are
  already running OSS and learning sensors unprompted. No evidence found of
  Airflow, Astronomer, or a competing orchestrator in the picture.

## Conflicts and gaps

No conflict found between the AE notes and public research — public signals
(job postings, stack requirements) corroborate the notes cleanly. Gaps,
stated honestly rather than papered over:

- BI tool, ingestion tooling, and cloud target for the data platform team are
  **unknown**.
- The specific business data domain behind the demo pipeline is **unknown** —
  the AE notes describe pipeline shape, not subject matter.
- Job titles for Andrew Rogers and B. Tekin could not be confirmed via
  LinkedIn or any other public source — **unknown**.

---

# Build directives

## Asset graph

Follow the AE's own suggested shape closely — this is a rare case where the
discovery notes already specify almost exactly what to build:

**Oracle/legacy source → Postgres landing → dbt staging → dbt marts → a
user-facing "platform status" view**, then layer Dagster+ concepts
(catalog, lineage, freshness, alerts, governance) visibly on top.

Suggested assets (12–16, group by layer):

- `legacy_oracle_extract` (kind: `oracle`) — represents the legacy Oracle
  system; partitioned daily, sub-daily arrival modeled via mock source
  timing (not partitions) to justify sensor-driven triggering.
- `external_feed_raw` (kind: `oracle` or generic source badge) — an
  event-arriving feed that a sensor watches, standing in for "trigger only
  when needed" vs. the old dumb schedule.
- `staged_readings` / `staged_reference` (kind: `postgres`) — Postgres
  landing zone for both sources.
- dbt staging models (`stg_*`) → intermediate models (`int_*`) → marts
  (`mart_*`), all via a real dbt Core project targeting DuckDB in demo mode,
  translator-overridden to kind `{"dbt", "postgres"}` (this is one of the
  rare cases where the badge matches the *real* target warehouse, not a
  disguise).
- `platform_status_report` — the final user-facing mart. This is the direct
  answer to Andrew's "communication mechanism" ask; give it a freshness
  policy and treat it as the money-shot asset.

Partition daily across the whole chain (matches their stated cadence
vocabulary of hourly/3-hourly/daily) — don't invent a second partition
dimension; no genuine second business axis is evidenced in the notes.

## Native integrations to use

- `dagster-dbt` for the transformation layer (rung 1 — mandatory per house
  rules, and directly requested in the demo-prep notes).
- Check the registry for an Oracle or Postgres ingestion component
  (`dagster-sling` or `dagster-embedded-elt`-style) before hand-rolling
  Oracle/Postgres extraction assets — search terms below.

## Community components to search for

- "oracle" (ingestion/extraction)
- "postgres" (ingestion/extraction)
- "sling" (Sling-based replication, native or community)

Search, don't guess IDs — record what's found (or the gap) in
`LEARNINGS.md`.

## Asset checks

At least three, each mapped to a named pain:

1. **Blocking** schema/row-count check on `staged_readings` before any dbt
   model runs downstream — maps to "operational complexity around views vs.
   materialized tables" (don't materialize on bad input).
2. Freshness/arrival check on `external_feed_raw` — maps to "trigger work
   only when needed."
3. A dbt-native test (uniqueness/not-null) surfaced as an asset check on a
   mart model — maps to "give users better visibility into data status."

Per house non-negotiables, validate at least one check against a
deliberately bad synthetic batch during the build so it's proven to catch
something real — but keep the live walkthrough green per the default
(no staged failure was requested here).

## Demo mode

- **What must be mocked:** Oracle connection, Postgres connection, and the
  external feed — no real ISO-NE credentials exist or should be implied.
  Engine is DuckDB under demo mode.
- **What can be real:** dbt Core executing real models against DuckDB;
  sensor logic; asset checks; freshness policies; automation conditions.
- **Asset kinds to display:** `oracle` for the legacy source, `postgres` for
  landing + dbt marts (translator override on `get_asset_spec`, matching
  their *actual* target stack — no disguise needed here).
- **Failure demonstration:** no (default). Nothing in the AE notes or the
  meeting context asks for a staged failure; this is a technical-credibility
  meeting with a team that already trusts Dagster's mechanics — show the
  green graph and talk through governance/observability instead.
- **Demo reset:** a script under `demo_data/` (outside Dagster) that resets
  the mock Oracle/Postgres/external-feed state files to their initial
  arrival timing.
- **Data realism notes:** keep row counts and cadence consistent with the
  AE's "hourly / every 3 hours / daily" vocabulary; no specific volumes were
  given, so don't invent precise figures — pick modest, demo-legible numbers
  and say they're illustrative if asked.

## Money shot

An event lands in the mock external feed → a sensor fires → only the
affected handful of downstream dbt assets recompute (not the whole hourly
batch) → the asset checks pass → the lineage graph shows clean dbt-native
dependencies → `platform_status_report` refreshes via
`AutomationCondition.eager()` and its freshness policy stays green. Then pan
to the catalog/lineage view and RBAC settings to make the Dagster+ pitch
concrete: *this is what you get on top of the orchestration you already
trust.*

## Capability talk track

- **Sensors + `AutomationCondition.eager()`** — replaces the "dumb schedule"
  directly; this is their stated #1 pain, make it the opening beat.
- **Blocking asset check on staged data** — answers the views-vs-materialized
  operational complexity; show that bad/incomplete data never reaches a
  costly full rebuild.
- **Freshness policy on `platform_status_report`** — this *is* Andrew's
  "communication mechanism" ask, almost verbatim.
- **dbt-native lineage + asset catalog** — the AE notes name this explicitly
  as a demo priority; frame it as the thing OSS doesn't give them today.
- **RBAC / audit logs / governance** — frame against NERC CIP CIP-003-9 /
  CIP-012-2 and the "security reviews can take months" reality; this is
  their actual procurement bottleneck, not a hypothetical.
- **Branch deployments / Git+PR workflow** — ties to their existing Git/PR
  process; safe CI/CD for a team that can't afford to test in production on
  critical grid-adjacent infrastructure.

## Explicitly out of scope

- Real Oracle or Postgres connections, and any real ISO-NE market/grid data.
- A staged live failure/recovery sequence — not requested, and this
  audience's context calls for credibility, not theater.
- Any specific named business data domain (settlement, metering, etc.) beyond
  what's needed to make the pipeline legible — the AE notes don't name one,
  and guessing wrong in front of a technical audience that already knows
  their own domain is a bigger risk than staying generic.
- BI-tool integration — no tool confirmed.
