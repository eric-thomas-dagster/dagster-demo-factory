---
company: ""
slug: ""
domain: ""
demo_date: ""
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

- **When:**
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

## Native integrations to use

<!-- dagster-dbt, dagster-snowflake, dagster-fivetran, dagster-sling... -->

## Community components to search for

<!-- Search TERMS, not IDs — the build routine searches the registry itself. -->

## Asset checks

<!-- At least three, each mapped to a pain named above. -->

## Demo mode

- **What must be mocked:** <!-- systems we have no credentials for -->
- **What can be real:** <!-- DuckDB, local files, public APIs -->
- **Planted anomaly:** <!-- which partition breaks, which check catches it -->
- **Recovery:** <!-- Assets are idempotent: recovery is just rematerializing
     the partition, because the SOURCE changed. Model late data as source
     arrival timing inside the mock. NEVER a heal asset, heal job, or reset
     object — a demo-control node in the lineage graph tells the prospect
     they're looking at scaffolding. -->
- **Demo reset:** <!-- how to re-run this demo a second time. A script or make
     target outside Dagster, operating on the mock source state. -->
- **Data realism notes:** <!-- cardinalities, date ranges, expected skew -->

## Money shot

<!-- The 20 seconds that sell it. What's on screen, what I say. -->

## Explicitly out of scope

<!-- Keeps the build routine from gold-plating past its time budget. -->
