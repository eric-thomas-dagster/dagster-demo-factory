---
company: ""
slug: ""
domain: ""
demo_date: ""        # "TBD" if no meeting is booked yet
demo_time: ""
attendees: []          # name — title — email
ae_doc: ""             # Drive URL, or "none"
ae_doc_modified: ""
overall_confidence: "" # high | medium | low
generated: ""
---

# <Company> — demo brief

## Demo thesis

<!-- Three to five sentences. The ONE thing this demo must prove to THESE
people. Not "show Dagster's features." Name their situation, their constraint,
and what changes if they believe us. Everything the build routine does keys off
this paragraph — if it's vague, the demo is vague. -->

## Meeting

- **When:** <!-- or "not booked yet — AE still working out timing" -->
- **Who's in the room:** <!-- name, title, and what each one cares about -->
- **Meeting type:** <!-- first demo / technical deep-dive / POC kickoff / architecture review -->

## Use case — confidence: <!-- high|medium|low -->

<!-- What are they actually trying to build or fix? -->

## Current stack — confidence: <!-- high|medium|low -->

| Layer | Tool | Source |
|---|---|---|
| Orchestration | | <!-- AE notes / job posting / blog / inferred --> |
| Warehouse | | |
| Transformation | | |
| Ingestion | | |
| BI | | |
| Cloud | | |
| CI/CD | | |

## Pain — confidence: <!-- high|medium|low -->

<!-- The specific broken thing. Quote the AE's phrasing where it's vivid —
that language should surface in the demo and in the room. -->

## Data domains

<!-- Claims, telemetry, orders, ad spend... plus volumes, cadence, SLAs. -->

## Public signals

- **Job postings:** <!-- count of open data roles + tools named in the JDs.
     The single best public evidence of a real stack. -->
- **Engineering blog / GitHub:**
- **Recent news:** <!-- funding, M&A, exec hires — especially a new VP Data,
     which usually means budget -->
- **Industry / compliance:** <!-- HIPAA, SOC2, PCI, GDPR -->
- **Orchestration signals:** <!-- Airflow pain, dbt adoption, warehouse migration -->

## Conflicts and gaps

<!-- Where the AE notes and public data disagree, list BOTH. Where we simply
don't know, say "unknown" — do not fill this with plausible invention. I say
these things out loud in a room. -->

---

# Build directives

## Asset graph

<!-- Name the assets. Group them into layers. Specify partitions and cadence
matching their real world. Target 12–25 assets. -->

## Demo shape

<!-- build-a-pipeline | orchestrate-existing-workloads | migration-both-states.
If they're mid-migration (SSIS->Fabric, Airflow->X, on-prem->cloud), specify
BOTH states: orchestrating the legacy estate they're moving off AND the new
platform, with lineage crossing between them in one graph. Orchestrating the
legacy side is the point, not a distraction — it makes the story additive
instead of rip-and-replace. If they mainly
want Dagster over what they already run (Fabric, Databricks, Airflow, Synapse),
say so — that changes the whole build toward external assets and
trigger-and-observe rather than a transformation graph. -->

## Native integrations to use

<!-- ONLY tools named in the AE notes or evidenced publicly. Do not assume dbt
or Snowflake. If a layer is unknown, say unknown — generic beats
specific-and-wrong. -->

## Community components to search for

<!-- Search TERMS, not IDs — the build routine searches the registry itself. -->

## Asset checks

<!-- At least three, each mapped to a pain named above. -->

## Demo mode

- **What must be mocked:** <!-- systems we have no credentials for -->
- **What can be real:** <!-- DuckDB, local files, public APIs -->
- **Asset kinds to display:** <!-- The prospect's stack, NOT the execution
     engine — e.g. snowflake, fivetran, dbt. DuckDB runs it; the UI shouldn't
     say so. dbt kinds come from the manifest adapter_type, so the translator
     needs overriding. -->
- **Everything materializes green.** Do NOT spec a planted anomaly, a corrupted
     partition, or any failure scenario — demos always work. Checks, freshness
     policies, and alerting are built and visible, and Eric talks through what
     happens when they fire in production. That talk track goes in the
     Capability talk track section below. -->
- **Data realism notes:** <!-- cardinalities, date ranges, expected skew -->

## Money shot

<!-- The 20 seconds that sell it. What's on screen, what I say. Against a
GREEN graph by default — lineage, checks configured, automation conditions,
kinds matching their stack. -->

## Capability talk track

<!-- What Eric says while the graph sits green. For each capability built in:
what it does, what happens when it fires, and which pain from above it answers.
This is the substitute for staging a live failure.

Include platform capabilities that need no build — Dagster+ native alerting to
Slack/Teams/email/PagerDuty is the main one. Don't spec custom alerting unless
there's a real gap the built-in policies don't cover; note that gap here if so. -->

## Explicitly out of scope

<!-- Keeps the build routine from gold-plating past its time budget. -->
