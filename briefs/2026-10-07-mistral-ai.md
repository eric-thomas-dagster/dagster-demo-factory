---
company: "Mistral AI"
slug: "mistral-ai"
domain: "mistral.ai"
demo_date: "2026-10-07"
demo_time: "12:00-12:30 America/New_York (30 min, Zoom, hosted by Tom Candino)"
attendees:
  - "Pierre-André Savalle — title unknown (presumed the 'Pierre' in the June discovery notes: newly landed data lead, ex-Poolside Dagster+ user) — pierreandre.savalle@mistral.ai"
  - "data-platform@mistral.ai — distribution list, membership unknown — invited, no response yet"
  - "Tom Candino — Dagster Labs (organizer)"
ae_doc: "https://docs.google.com/document/d/1h1RVQdUsfMjp6qgmLvKfZV4WNP-uycF_UfjrGejFqIg/edit (Mistral Disco - June 8, 2026, owner Joe Bower); plus customer-authored https://docs.google.com/document/d/16-iU2Ew3qQ4dwBh0VAkJyDH3El86DVQY9nQB7sEegng/edit (Dagster Cloud x Mistral, Pierre-André Savalle)"
ae_doc_modified: "2026-06-08 (disco notes); 2026-06-15 (customer question doc)"
overall_confidence: "medium"
generated: "2026-09-30"
---

# Mistral AI — demo brief

## Demo thesis

Mistral's data lead wants to standardize a heterogeneous, multi-team stack on Dagster without a big-bang move, and has already told us what breaks at scale: science teams create "asset collections" (50 datasets, then 50 derived, then 50 more) and tracking "gets a bit messy." This 30-minute session for the Science group has to prove that one YAML-instantiated collection of many datasets stays navigable as it snowballs: filter, traverse lineage, backfill a slice, and drive all of it from the `dg` CLI so agents can do it too. The proof is that the 150th dataset is one more line of YAML and still findable, not that Dagster has features.

## Meeting

- **When:** Wed 2026-10-07, 12:00–12:30 ET. Calendar event "Dagster+Mistral Science Group", one-off (not recurring).
- **Who's in the room:** Pierre-André Savalle (Mistral; cares about multiple code locations, asset collections, `dg` CLI for agents, run tracking). `data-platform@mistral.ai` list is on the invite; individual science-group members are unknown. Dagster side: Tom Candino (organizer), Eric.
- **Meeting type:** Focused product walkthrough for a new internal team at an existing exploratory prospect. The invite description is the agenda: "Dagster UI and CLI/MCP flows: asset catalog/graph, and how to quickly filter and traverse lineage; partitioning; runs and backfills."

## Use case — confidence: medium

Primary use case was "Unknown" in the June disco; the science group's datasets are the likely first workload. What is known (AE notes + customer doc): many pipelines to consolidate into an asset-centric setup; lots of teams with different data shapes; multiple code locations from day one; LLM-era spin-up/spin-down of many similar assets ("10 datasets, process, then 20 datasets"); searchable catalog and run tracking that agents can plug into. What the science group specifically runs is not in any doc — assumed to be dataset curation / evaluation datasets for model training (inferred from public "Mistral Science" hiring, low confidence).

## Current stack — confidence: low

| Layer | Tool | Source |
|---|---|---|
| Orchestration | Flyte 2.0 (hosted SaaS) named as competing tool; "legacy orchestrators" being migrated away from (name not published) | AE notes (competitor); public job posting (legacy, unnamed) |
| Warehouse | unknown | — |
| Transformation | unknown | Data/Analytics Engineer JD mentions "ETL processes" but no tool seen |
| Ingestion | unknown | — |
| BI | unknown | — |
| Cloud | Own data centers (Mistral Compute) + cloud; Kubernetes and SLURM training environments | Public job posting (snippet via search); funding news |
| CI/CD | unknown | — |

Also from the customer doc: runs integrate "Spark, kubernetes, SLURM, etc." and they want direct links to Grafana from the run page. Big-data tooling (Hadoop, Spark, Hive) appears in a Paris "Software Engineer, Data" posting.

## Pain — confidence: high (customer's own words)

- "Often we have collections of assets that we process together to create a new collection. Say, we start with 50 assets, then derive 50 more, then 50 more, etc. Tracking can get a bit messy. Partitions don't seem like a good fit." (Pierre-André, customer doc)
- AE notes: "POC looks nice, but then in prod things balloon up"; "asset groups were okay, but beyond that, that's it"; "Downstream assets for every use-case, starts to snowball."
- Cross-location dependencies "still cumbersome" (marked P0 by the customer).
- Wants to "manage everything from `dg` CLI", backfills/partition ranges, catalog search by name/description, so agents can automate.

## Data domains

Unknown for the science group. Assumed generic ML-adjacent datasets (source corpora, cleaned/filtered corpora, evaluation sets) rather than anything domain-specific. Volumes and cadence: unknown. Do not invent numbers.

## Public signals

- **Job postings:** Several Data roles live: Research Engineer, Data Infrastructure (multiple locations/listings); Software Engineer, Data (Paris); Data/Analytics Engineer (Paris). The Data Infrastructure JD describes "a strategic transition from legacy scheduling to modern orchestration", "architecting the migration away from legacy orchestrators", "multi-cluster orchestration and cloud-bursting", Kubernetes and SLURM training environments, and "metadata and lineage ... as data and model pipelines grow in complexity." Job pages themselves were blocked by the sandbox proxy; content is from search-result summaries only.
- **Engineering blog / GitHub:** nothing relevant found on orchestration.
- **Recent news:** €3B Series D announced Sep 8, 2026 (Samsung-led, >€21B valuation); $830M debt financing for a data center near Paris (13,800 GB300 GPUs); €1.2B Sweden data center partnership with EcoDataCenter opening 2027. (Note the AE notes say "$850M in funding in March"; public reports say $830M debt, see Conflicts.) A new "Mistral Science" division is hiring Discovery Scientists (physics, chemistry, materials) and Lead AI Scientists (finance, materials, biology).
- **Industry / compliance:** EU-based, GDPR; EU AI Act GPAI obligations effective Aug 2, 2026; EU data residency is a selling point. Relevant to lineage/provenance for training data.
- **Orchestration signals:** explicit legacy-orchestrator migration in JDs; Flyte named as competitor; customer's Flyte-style `LinkEvent` request suggests familiarity with Flyte.

## Conflicts and gaps

- **Funding figure:** AE notes say "$850M in funding in March." Public reporting found says $830M in debt financing for the data center, plus a €3B Series D on Sep 8. Not reconciled; don't quote $850M in the room.
- **Who "Pierre" is:** AE notes say "Pierre"; the invite attendee is Pierre-André Savalle. Treated as the same person; his title is not stated anywhere I could read.
- **Audience shift:** the disco notes are from the data lead (June 8, ~16 weeks old). This meeting is for the "Science Group". Their needs may differ; nothing written by or about that group was found.
- **Customer doc is authoritative on technical asks, but it dates from June.**
- **Flyte 2.0 as the competitor** comes from AE notes only; no public corroboration.
- Unknown: warehouse, transformation, ingestion, BI, current orchestrator name, science-group datasets, volumes, cadence, number and identity of attendees. Not researched further: LinkedIn (unavailable), Mistral's GitHub org.
- No discovery doc exists for the Science Group itself.

---

# Build directives

## Asset graph

Sized large on purpose: the thesis is about collections that snowball, so a thin graph undersells it. Target ~40–60 assets, all from a few YAML component instances. Layers, all named with generic dataset vocabulary because the domain is unknown:

1. `source_datasets` — ~12 raw dataset assets (one collection). Mapping table in `defs.yaml`, one line per dataset.
2. `curated_datasets` — ~12 derived, one per source (filtered/deduplicated).
3. `derived_collection_a` — ~12 more, derived from curated (e.g. evaluation slices). The "50, then 50 more" snowball, visibly three generations deep.
4. `dataset_catalog` — 2–3 summary assets across the collections (per-collection manifest/index).
5. A small "runs on compute" slice: 2–3 assets that execute on Kubernetes compute via Pipes (see integrations), to show run tracking and links to external UIs.

Partitions: the customer said partitions are a poor fit for the collections, so do not partition layers 1–3. Partition only where genuinely natural: a daily-partitioned ingest slice (layer 1 subset, ~3 assets) so the "partitioning" and "backfills" agenda items have something real to show. Flag this as an assumption in the README.

Groups: one per layer, plus `team_*` owner metadata (team names generic, e.g. `science`, `data-platform`). Adding an asset must be one more YAML line.

## Fidelity

**stubbed** for the small number of hand-written Python bodies. Collection assets get no-op bodies via their component. Reason: lineage, filtering, backfill and CLI are the story; synthetic numbers add nothing and there is no stated domain to make them realistic.

## Demo shape

**build-a-pipeline**, specifically the "collections at scale" shape. Not a migration demo: their legacy orchestrator is unnamed, so build nothing for it (conversation only).

## Native integrations to use

Only what evidence supports. Kubernetes is evidenced publicly (JD) and in the customer doc, so a Pipes-to-Kubernetes compute slice is justified. Everything else: unknown, prefer generic. Do not assume dbt, Snowflake, Spark, or Databricks. Do not badge the warehouse layer; use generic kinds only where evidenced (`kubernetes`, `python`).

## Community components to search for

Search terms, run them with `--json` and record literal commands if a custom component ends up being needed: "dataset collection", "asset factory", "yaml assets", "bulk assets", "kubernetes pipes", "pipes kubernetes", "slurm", "grafana", "external link", "asset catalog", "automation condition applicator", "cron schedule". The reference shape is one collection-style component instance with an explicit mapping table (`assets_by_task_key`-style), not N instances.

## Asset checks

At least 3, tied to the customer's stated pains:
- **Blocking:** per-collection completeness check on a curated layer asset (all expected source datasets present before derived collections compute). Answers "tracking gets messy."
- **Blocking:** schema-shape check on one derived collection gating downstream.
- **Warning:** catalog-manifest row-count reconciliation across the three generations.

A multi-asset check spec generated once and applied across a collection is on-message (one definition, many assets). All green.

## Demo mode

- **What must be mocked:** Kubernetes cluster access for the Pipes slice (subclass the real component/client at the launch seam; no cluster available).
- **What can be real:** local Parquet/JSON files or DuckDB under `demo_data/` for any stub outputs.
- **Asset kinds to display:** `kubernetes` on the compute slice; `python` on hand-written assets; otherwise none. No guessed warehouse badge.
- **Everything materializes green.** No planted anomaly, corrupted partition, or failure scenario.
- **Data realism notes:** dataset names generic and domain-neutral; fixed seed; fixed counts.

## Three buckets

- **IN CODE:** YAML-instantiated asset collections across three generations; asset metadata (owner, team, tags) for filtering; ~3 checks; freshness policy on the catalog assets; eager automation on derived collections gated on blocking checks; one schedule; daily-partitioned ingest slice; Pipes-to-Kubernetes compute slice.
- **DAGSTER+:** asset catalog search and filtering, lineage traversal, runs and backfill UI, branch deployments, multiple code locations and how they appear together, Slack notifications, run history, RBAC. Demonstrate only.
- **CONVERSATION:** cross-code-location dependency ergonomics (customer's P0), `LinkEvent`-style external links (ask whether it exists; build nothing), migration off their legacy orchestrator, a `dg` TUI, asset-collection grouping roadmap (customer referenced dagster issue #14530), Flyte comparison. Write no code for these.

Note on `dg` CLI: the customer asked about `dg api` run launch (backfills, partition ranges) and `dg api asset search`. Do not assume they exist from this brief; verify current `dg` command forms by running them and report what works in the notification.

## Demo name

**"150 Datasets, One Line Each"**

## Money shot

Open the asset graph with three generations of dataset collections on screen. Filter by group and owner in the catalog, traverse lineage from one source dataset to everything derived from it, then backfill a partition range from the UI. Then run the same discovery from the terminal with `dg` (list, search, launch). Line: "You said tracking gets messy at 50, then 50 more. This is 40-plus assets, and adding the next collection is one YAML entry, findable in the catalog and operable by an agent."

## Capability talk track

- **Checks:** blocking completeness check stops a derived collection computing on a partial source collection. In production it fires to Slack through Dagster+ alerting, with nothing built here. Answers the "tracking gets messy" pain.
- **Freshness policy on the catalog assets:** answers "how would we know a collection went stale."
- **Automation conditions:** eager recompute across generations, gated on checks. Show what would run and when; contrast with hand-rolled triggers.
- **Partitioning:** show the daily ingest slice; acknowledge the customer's point that partitions are the wrong tool for collections and that's why the collections are not partitioned.
- **Run tracking:** run history and Kubernetes-launched runs with streamed logs; Pipes messages. External UI links are a conversation item.
- **Alerting / RBAC / branch deployments:** Dagster+ platform features, demonstrated not built.

## Explicitly out of scope

- No second code location built in this repo (describe and show in Dagster+ only, or leave to the conversation).
- No SLURM integration unless a registry component covers it as-is; no custom component for it.
- No custom alerting, no `LinkEvent` workaround, no legacy-orchestrator assets.
- No warehouse/BI/transformation layer; stack is unknown.
- No guessing the science group's domain datasets.
