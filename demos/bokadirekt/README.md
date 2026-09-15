# Bokadirekt — Dagster demo

## "Ship the Second Deployment"

Noel Mattar is a hands-on technical champion already self-serve trialing
Dagster: he built his own dbt-on-GCP project under Dagster Starter and hit
the wall the free tier is designed to surface — Starter doesn't support a
second (production) deployment, so his branch → merge-to-prod workflow
errors. He's presenting Dagster to his own team this month. This demo has
to prove the next tier removes that one specific blocker on a project
shaped like the one he already built, not tour generic features he'd have
to translate into his own environment.

## The pitch

This is a **self-serve PLG follow-up**, not an AE-sourced enterprise
evaluation — the brief itself flags this prominently. The graph mirrors
Noel's own project structure almost exactly: a "clean" (staging) dbt
folder, an "entity" (mart) dbt folder, a Python model, and one asset
standing in for "the model that ingests data." The money shot is entirely
a Dagster+ platform capability (branch deployments / multiple
environments), demonstrated live, never hand-built in this repo.

## Getting started

```bash
uv sync
source .venv/bin/activate
dg dev
```

Open http://localhost:3000. **No environment variables or manual setup
are required.** `BOKADIREKT_DEMO_DUCKDB_PATH` defaults to a
project-relative DuckDB file; the simulated upstream booking-platform
tables (`raw.raw_bookings`, `raw.raw_specialists`) seed themselves the
first time the package imports — see "Fidelity" below.

## Integration mapping — every system, its real component

| System | Component | Rung | Escalation reasoning |
|---|---|---|---|
| **dbt Core** (transformation, confirmed directly — Noel showed his own dbt project) | `dagster_dbt.DbtProjectComponent`, subclassed as `BokadirektDbtComponent` | 1 (native), subclassed for kind-badge only | Runs for real in both demo and live mode — no I/O seam needed. Subclass only overrides `get_asset_spec` to badge `dbt`+`bigquery` instead of the manifest-derived `duckdb`. |
| **BigQuery** (warehouse, "this GCP project" — medium-confidence, never named directly) | `kinds={"bigquery"}` badge on the dbt layer + `raw_bookings_feed` | n/a | Engine is DuckDB in demo mode; badge reflects Noel's own framing, **flagged as an assumption** below. `dagster-component search "bigquery" --json` returned resource/ingestion/sink components for moving data *into/out of* BigQuery (`bigquery_resource`, `bigquery_load_from_gcs_asset`, etc.) — none fit "observe an existing table for freshness," which is what this build needs; a plain `@dg.observable_source_asset` is core Dagster, not an integration surface, so the escalation ladder doesn't apply to it (same reasoning as the plain `@asset` used for `specialist_utilization_daily`). |
| **The ingestion model** ("a dbt model that ingests data") | `raw_bookings_feed`, a core-Dagster `@dg.observable_source_asset` | n/a | Modelled as external/observable per CLAUDE.md's "orchestrating existing workloads" guidance — Bokadirekt's booking platform owns this data; Dagster observes freshness, never materializes it. |
| **BI tool** | none — no kind badge anywhere downstream | n/a | No BI tool is named anywhere in the AE notes or public research. |

No registry component was needed for BigQuery beyond the badge — searched
per the escalation ladder, nothing fell through to rung 4, so no
`component-feedback/` entry.

## Fidelity: dbt runs for real, the Python rollup is stubbed

Per the brief, **Axis 2 is stubbed (default)**: `specialist_utilization_daily`
is the one hand-written Python asset, mirroring the Python model already
in Noel's own project, and its body does nothing beyond confirming its
inputs materialized — no utilization-rate calculation, no fabricated
numbers. **Axis 1 (dbt) still runs for real regardless of fidelity** — real
dbt Core, real `schema.yml` tests, against the seeded DuckDB tables that
stand in for BigQuery.

Flipping to Bokadirekt's real BigQuery project is a `dbt_project/profiles.yml`
target change (`live` output already stubbed with the expected
`BOKADIREKT_GCP_PROJECT` / `BOKADIREKT_GCP_KEYFILE` env vars) — same dbt
project, same tests, same asset graph, same YAML.

## The asset graph

Seven assets, one lineage graph, matching the brief's own explicit list:

- **`ingestion`** (1 asset, badged `bigquery`) — `raw_bookings_feed`, an
  observable source asset with its own opt-in polling sensor
  (`raw_bookings_feed_observation_sensor`, off by default — see
  "Observation" below).
- **`raw` (implicit dbt source)** — `raw_specialists`, the dbt source
  node dagster-dbt auto-creates for the specialist roster table; no
  dedicated Dagster asset needed beyond the dbt source declaration, since
  the brief names only one ingestion-shaped asset.
- **`dbt`** (4 assets, badged `dbt`+`bigquery`, real dbt Core) —
  `staging/stg_bookings`, `staging/stg_specialists` (unpartitioned),
  `marts/dim_specialists` (unpartitioned), `marts/fct_bookings_daily`
  (daily-partitioned — the brief's one named partition axis).
- **`reporting`** (1 asset, no kind badge, plain `@asset`, daily-partitioned) —
  `specialist_utilization_daily`, stubbed per Axis 2.

## Asset checks

| Check | Asset | Severity | Catches |
|---|---|---|---|
| `stg_bookings_completeness` | `staging/stg_bookings` | **Blocking** | Fewer than 20 bookings on file for the day, or any null `booking_id` — the entity layer refuses to compute on an incomplete or malformed feed. |
| `stg_specialists_schema_pii` | `staging/stg_specialists` | Warning | Missing expected columns, or an unannounced PII-shaped column (national ID, email, phone, address) — the GDPR guardrail for this feed. |
| `fct_bookings_daily_completeness` | `marts/fct_bookings_daily` | **Blocking** | A materialized day with zero bookings — gates `specialist_utilization_daily` from computing on empty data. |
| 9 dbt-native `not_null`/`unique` tests | `stg_bookings`, `stg_specialists`, `dim_specialists`, `fct_bookings_daily` | dbt default | Missing/duplicate keys, auto-surfaced as Dagster asset checks with zero extra code. |

All checks compute genuine results against the seeded/dbt data (not
hardcoded passes) — `validate_e2e.py` asserts they evaluate to `True`, not
just that they ran. Per house rules, everything stays green: no planted
failure anywhere in this build.

## Freshness, automation, observation, and schedule

- **Freshness policy**: `fct_bookings_daily` (fail at 24h, warn at 12h) —
  the asset a Bokadirekt data person would page someone about.
- **Automation conditions**: `AutomationCondition.eager()` on the entire
  entity layer plus staging, per the brief; `stg_bookings` and
  `specialist_utilization_daily` additionally gate on
  `all_deps_blocking_checks_passed()` — a bad bookings day doesn't
  silently propagate forward.
- **Observation sensor**: `raw_bookings_feed_observation_sensor` polls the
  booking-platform export every 5 minutes so a booking that lands outside
  any Dagster-triggered run still produces an `AssetObservation` — **off
  by default** (`DefaultSensorStatus.STOPPED`), matching the
  workspace-component convention of opt-in polling sensors, toggle it on
  in the Sensors tab to show it live.
- **Schedule**: `bokadirekt_morning_marts_schedule` computes the prior
  day's bookings fact and utilization rollup at 07:00 Europe/Stockholm.
  **Assumption**: no cutover time is named in the brief; Stockholm is
  Bokadirekt's real HQ timezone (confirmed, not guessed).
- **Retry policies**: none. Nothing in this graph is a genuinely flaky
  source — a decorative retry on a deterministic seeded table would
  invite a question we'd lose.

## Three buckets

1. **Implemented in code**: the seven-asset graph (real dbt Core with
   native tests plus three custom checks, one freshness policy, eager
   automation gated on blocking checks, the observation sensor, the
   morning schedule).
2. **Handled by Dagster+, demonstrated not built**: **branch deployments
   and multiple environments** — the headline moment, the actual fix for
   Noel's blocker. Also demonstrated, not built: CI/CD gating on PRs,
   alerting to Slack, RBAC/teams (relevant once he brings his team in),
   restart-from-failure.
3. **Conversation only, nothing built**: Bokadirekt's real production data
   estate (unknown — no evidence any orchestrator runs there today, so no
   "replacing X" narrative); scaling this pattern to real
   specialist/booking volumes; any migration off an incumbent tool.

## `defs/` file count

**2 YAML, 5 Python** (plus boilerplate `defs/__init__.py`). By file count
Python is the majority, but the ratio that matters — which files *define
assets* — runs the other way: **5 of 7 assets are YAML-instantiated** (2
dbt `defs.yaml` files covering 4 dbt assets, plus the implicit
`raw_specialists` dbt-source node), and only two hand-written asset
definitions exist (`raw_bookings_feed`, `specialist_utilization_daily`),
each justified below:

- `defs/transformation/staging/defs.yaml`,
  `defs/transformation/marts/defs.yaml` — the whole dbt layer (4 assets),
  `BokadirektDbtComponent` instantiated twice (unpartitioned staging vs.
  partitioned marts).
- `defs/ingestion/raw_bookings_feed.py` — **justified**: no ingestion tool
  is named in the brief for this feed (it's Bokadirekt's own booking
  platform); `@dg.observable_source_asset` plus its sensor is core
  Dagster, not an integration surface a registry component would cover.
- `defs/reporting/specialist_utilization_daily.py` — **justified**: no
  external system here, so no component applies; a one-object plain
  `@asset` is the right size for this Axis-2 rollup, matching the Python
  model already in Noel's own project.
- `defs/transformation/staging/template_vars.py`,
  `defs/transformation/marts/template_vars.py` — **justified**: Jinja
  (YAML template expressions) has no `&` operator and `dg`'s template
  scope doesn't expose `AutomationCondition`/`FreshnessPolicy` composition
  directly; exposing the composed Python values as `@dg.template_var`s is
  the documented sanctioned pattern (same as demos/partners-fcu).
- `defs/checks/stg_bookings_completeness.py`,
  `defs/checks/stg_specialists_schema_pii.py`,
  `defs/checks/fct_bookings_daily_completeness.py` — **justified**:
  asset-check assertion logic is business logic; no registry component
  covers a declarative check against an arbitrary DuckDB query.
- `defs/automation/morning_marts_schedule.py` — **justified**: the
  registry's `cron_schedule` component's partitioned-job mode can't
  express a specific hour alongside a `partitions_def` (same registry gap
  recorded for demos/partners-fcu, demos/rvu-tempcover — not
  re-litigated here); the one-function native
  `build_schedule_from_partitioned_job` call is used directly instead.

`components/dbt_project.py`, `components/partitions.py`,
`demo_data/warehouse.py`, and `demo_data/seed.py` are components/support
code, not `defs/` — only `defs/` is measured for the YAML-first ratio.

## Registry search record

Per the brief's "Community components to search for" list and CLAUDE.md's
escalation ladder (at least three searches before writing custom code):

```
dagster-component search "bigquery" --json
dagster-component search "dbt" --json
```

`bigquery` returned `bigquery_resource`, `bigquery_dry_run_check`,
`bigquery_export_to_gcs_asset`, `bigquery_load_from_gcs_asset`,
`bigquery_ml_train_asset` — all move-data-in-or-out primitives, none of
which fit "observe an already-existing table for freshness," which is all
this build needs from BigQuery. `dbt` confirmed native `dagster-dbt`
(`DbtProjectComponent`) is the right rung-1 answer, used directly.

No custom component was written for this build — nothing fell through to
rung 4, so no `component-feedback/` entry was needed.

## Which parts run where

The dbt layer's DuckDB file is local — it exists for the lifetime of one
process (or one Dagster+ Serverless container; **Serverless storage is
ephemeral**, so a fresh container starts with an empty warehouse each
run, reseeded on import). **Give the interactive demo locally with `dg
dev`** — the multi-materialization walkthrough only persists across runs
there. Treat the deployed Dagster+ code location as proof the project is
real and loads correctly. **The actual money shot (branch deployments,
second production deployment) lives entirely in Dagster+ itself, not in
this repo** — it works identically regardless of which container the
code location happens to be running in.

## Assumptions

- **`kinds={"bigquery"}`** — Noel said "this GCP project" on the 9/9 call
  but never named BigQuery directly; medium confidence per the brief.
- **Timezone (`Europe/Stockholm`) and the 07:00 schedule hour** —
  Stockholm is Bokadirekt's real confirmed HQ, so the timezone is not a
  guess; the specific hour is this build's own assumption, no cutover
  time is named in the brief.
- **Freshness thresholds (24h fail / 12h warn)** on `fct_bookings_daily` —
  no real numbers exist in the brief; this build's own plausible default.
- **No production orchestrator narrative** — the brief explicitly warns
  against inventing a "replacing X" story; none is told here.
- **Small, deterministic row counts** (40 specialists, ~30-54
  bookings/day over 21 days) — per the brief, "a walkthrough for one
  technical person, not a volume story."
- **No planted failure anywhere** — every check, freshness policy, and
  automation condition shown is real and currently green; the whole point
  of this meeting is a clean flow, the opposite of what Noel hit on his
  own trial.
