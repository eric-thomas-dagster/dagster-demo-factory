---
company: "Harmattan AI"
slug: "harmattan-ai"
domain: "harmattan.ai"
demo_date: "2026-10-09"
demo_time: "09:00-10:00 America/New_York (15:00-16:00 Paris)"
attendees:
  - "Paul Clisson — title unknown — paul.clisson@harmattan.ai (accepted)"
  - "Naomi Mastico — Dagster Labs AE, organizer — naomi@dagsterlabs.com"
  - "Eric Thomas — Dagster Labs, presenter — eric.thomas@dagsterlabs.com"
ae_doc: "none"
ae_doc_modified: ""
overall_confidence: "low"
generated: "2026-10-07"
---

# Harmattan AI — demo brief

> **No AE discovery notes were found.** Drive search (company name, domain,
> attendee surname, "harmattan.ai") and the recent-files listing returned
> nothing. The invite was created today (2026-10-07), so notes may not exist
> yet. Everything below comes from the calendar invite and public research.
> Build generic where unknown.

## Demo thesis

Harmattan AI is a two-year-old, $1.4B-valuation defense startup whose data
engineers (per a public job posting) own the layer feeding ML teams: terabytes
of raw, unstructured video and sensor streams from field and flight operations,
which must be aligned in time and space, cleaned, documented and versioned into
training datasets. This demo has to prove that Dagster turns that dataset
pipeline into a graph of partitioned assets with checks, so ML teams can trust
which data version a model was trained on and rerun just the slice that changed,
without hand-built glue. It must also show that Dagster+ Hybrid keeps compute and
data inside their own infrastructure, which a defense company will care about.
Confidence is low: we do not know their orchestrator, cloud or storage, or who
Paul is. The story is the one the public job posting supports.

## Meeting

- **When:** 2026-10-09, 09:00-10:00 America/New_York (one hour). Created
  2026-10-07, so booked with very short notice.
- **Who's in the room:** One external attendee: Paul Clisson
  (paul.clisson@harmattan.ai), title unknown, accepted. His role and
  technical depth are unknown. Possibly others join.
- **Meeting type:** Titled "Dagster+ Pro <> Harmattan.ai". Not a POC, not
  recurring. Treat as a first demo/evaluation. The "Pro" in the title suggests
  the AE is already positioning Pro tier (inference, not confirmed).

## Use case — confidence: low

Not stated by the AE. Public inference: orchestrating ML data preparation for
autonomous drone systems. The job posting describes ingesting raw unstructured
field data from video and sensor streams, aligning multiple streams temporally
and spatially so paired data is usable for training, and producing clean,
documented, versioned datasets for ML teams. The posting's title references
"detect / track" and "distillation" (model distillation), which suggests
perception-model training data. Unknown whether Dagster is for this, for
platform/infra, or for something else entirely.

## Current stack — confidence: low

| Layer | Tool | Source |
|---|---|---|
| Orchestration | unknown | no signal found |
| Warehouse | unknown | — |
| Transformation | unknown (Python/ML-style dataset prep implied) | job posting (secondhand) |
| Ingestion | raw video + sensor streams from field/flight ops | job posting (secondhand) |
| BI | unknown | — |
| Cloud | unknown | — |
| CI/CD | unknown | — |

The job posting page itself (choppingblock.ai) was blocked from this
environment; its content is known only through a search-result summary. Verify.

## Pain — confidence: low

No AE phrasing exists. Inferred from the data-engineer role, not stated by
anyone at Harmattan: dataset versioning and provenance for ML training, and
time-aligning multi-stream data. Do not present these as their stated pains.

## Data domains

Video and sensor/telemetry streams from drones and field operations, labelled
frames, detection/tracking training sets. Volumes quoted publicly: "terabytes
of raw, unstructured data." Cadence unknown. Public product lines (three:
training-focused system, surveillance (ISR) drone, weapons-interceptor
drone) are a reasonable partition dimension. Do not use any real programme
names, contracts or customer data.

## Public signals

- **Job postings:** At least one Data Engineer role ("Detect/Track
  Distillation"), Paris/Lausanne/Zurich. Other open roles seen are embedded,
  robotics, electrical, flight-ops, AI research interns, so data hiring is
  small relative to the company. No orchestrator named in what I could see.
- **Engineering blog / GitHub:** none found.
- **Recent news:** $200M Series B led by Dassault Aviation (Jan 2026),
  $1.4B valuation, $242M total raised; strategic partnership embedding their
  AI in Dassault's future air combat systems; programmes of record with the
  French MoD (1,000 drones) and UK MoD (3,000); partnership with Morocco's
  Royal Armed Forces (June 2026). Founded April 2024, Paris.
- **Industry / compliance:** Defense. Plausible data-residency, sovereignty and
  export-control sensitivity (industry-typical, not confirmed for them).
  Offices in Paris, Lausanne, Zurich, Orly.
- **Orchestration signals:** none found.

## Conflicts and gaps

- No AE notes, so nothing to conflict with. Everything is a gap: orchestrator,
  cloud, storage, ML tooling, team size, who Paul is, why Pro.
- The posting was read through a search summary only.
- Dagster Labs/Prefect context: the merger is public (July 2026); a defense
  buyer may ask about it. Not researched further.

---

# Build directives

## Asset graph

About 10-12 assets, one focused ML-data-prep pipeline. A thin, readable graph
fits a single-pipeline story. Assets, in layers:

- **field_ingest:** `raw_flight_video`, `raw_sensor_telemetry`
- **alignment:** `time_aligned_streams`, `spatially_registered_frames`
- **labeling:** `labeled_detections`, `labeled_tracks`
- **datasets:** `detect_track_training_set`, `distillation_training_set`
- **release:** `dataset_release_manifest` (versioned, documented), `model_eval_report`

Partitions: daily (`flight_date`) crossed with `platform` (training, isr,
interceptor) via `MultiPartitionsDefinition` on the ingest and alignment layers;
datasets layer daily. Cadence is an assumption.

Group names by layer. No stack kinds, since the stack is unknown (plain
python-ish badges only if a real component supplies them).

## Fidelity

stubbed. Bodies are `pass`; hand-written Python only for derived assets like the
release manifest. Metadata (frame counts, aligned-stream drift ms, dataset
version) as static plausible metadata, flagged synthetic. No real imagery or
real defense data.

## Demo shape

build-a-pipeline (no legacy estate is known). Do not invent a legacy
orchestrator.

## Native integrations to use

None confirmed. Do not assume dbt, Snowflake, Databricks or any warehouse.
Add a cloud/object-store layer only if the registry offers a generic,
non-vendor component; otherwise leave storage unspecified.

## Community components to search for

Search terms (run all, with `--json`, and record them): `video`, `sensor
telemetry`, `object storage`, `dataset versioning`, `ml dataset`, `mlflow`,
`model registry`, `labeling`, `annotation`, `kubernetes`. If an external system
has no match, record a component-feedback entry with literal commands run.
Any custom component must be named for a domain concept (for example a flight
log ingest) and never a generic name.

## Asset checks

At least three, tied to the inferred pains:
1. **Blocking:** `time_aligned_streams` stream-sync tolerance (video vs sensor
   offset within bound) gates labeling.
2. **Blocking:** `labeled_detections` label coverage/completeness gates the
   training set.
3. **Warning:** `detect_track_training_set` class-balance bounds.
4. **Warning:** `dataset_release_manifest` completeness of documentation fields.
All real and passing against deterministic seeded data.

## Demo mode

- **What must be mocked:** all field ingest sources and any storage.
- **What can be real:** local files and DuckDB or Parquet under `demo_data/`.
- **Asset kinds to display:** none vendor-specific; no stack confirmed.
- **Everything materializes green.** No planted anomalies or failure
  scenarios.
- **Data realism notes:** synthetic only; seeded; small frame/stream counts;
  3 platforms; 30 days of partitions.

## Three buckets

- **IN CODE:** partitioned asset graph, the four checks, freshness policy on
  the training set, eager automation gated on blocking checks, metadata on every
  asset, one schedule.
- **DAGSTER+:** Hybrid deployment (compute and data stay in their environment),
  RBAC/SSO/audit, alerting, branch deployments, lineage and catalog UI, run
  history, backfill UI, restart from failure.
- **CONVERSATION:** air-gapped or sovereign deployment specifics, export-control
  questions, Prefect/Dagster roadmap, integration with their training
  infrastructure. Build nothing.

## Demo name

"Flight Data to Training Set"

## Money shot

Click `detect_track_training_set`: the lineage runs back through labeling and
time alignment to raw flight video and sensor streams, with dataset version and
stream-sync metadata on the node, and the blocking sync check that gates it.
Say: "Every dataset your ML team trains on has a traceable, checked history,
and you can rerun just one flight day for one platform." Against a green graph.

## Capability talk track

- **Partitions (date x platform):** rerun one day/platform when labels or
  alignment change. Answers dataset rework.
- **Blocking checks:** training set never builds on misaligned or unlabeled
  data.
- **Freshness policy:** know when a training set is stale before ML does.
- **Eager automation:** relabeling flows downstream by itself.
- **Dagster+ (show, don't build):** Hybrid, alerting, RBAC/audit, branch
  deployments.
- Frame all of this as what the job posting describes, and ask Paul to correct
  the picture.

## Explicitly out of scope

Real or realistic defense systems detail, any weapons-related content, named
programmes or customers, custom alerting, model training code, real stack
vendors, and any failure staging.
