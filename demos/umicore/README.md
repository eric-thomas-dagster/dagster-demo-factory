# Umicore — Dagster demo

## "Untangling the Spiderweb"

Umicore runs a deliberate data-mesh architecture across four business units
with no data-aware orchestrator underneath it: data refreshes on a fixed
clock instead of on actual upstream completion, so a late job leaves
everything downstream stale until the next window, and (per the business
case) at least one team's workflow is broken on any given day with no
central place to see it. This demo proves coexistence is real, not a
compromise: **Stonebranch/Ansible stays master for infra-deployment
scheduling; Dagster owns data freshness, quality, and cross-business-unit
lineage** — a Stonebranch-triggered ADF pipeline shows up in Dagster's
lineage graph as an observed asset, not a black box, with the real
Dagster-owned pipeline visibly downstream of it.

**No meeting is booked for this brief** (see "Assumptions" below) — this
was built from an internal AE business-case document referencing an August
5, 2026 discovery call, not a scheduled demo. Confidence throughout is
`medium`; specific low-confidence items are called out inline.

## The pitch

Wauter described the cross-business-unit dependency web as "a spiderweb
that is only getting more tangled." Anthony gave the concrete example: a
3-job sequential chain where, if the first job runs long, the other two sit
stale until the next scheduled window with no automatic recovery. This
demo's money shot walks that exact chain live — materialize the first
business-unit asset and watch the next two recompute immediately via eager
automation, no waiting for a fixed window — then pans to the
Stonebranch-triggered legacy pipeline sitting in the same lineage graph,
observed, never triggered, making the coexistence promise concrete.

**Every external system in the graph runs through the real, corresponding
Dagster integration** (native `dagster-dbt`, native `dagster-databricks`,
the community registry's `azure_data_factory`, native `dagster-powerbi`),
with only the network boundary mocked — see "Integration mapping" below.

## Getting started

```bash
uv sync
source .venv/bin/activate
dg dev
```

Open http://localhost:3000. **No environment variables or manual setup are
required.** `UMICORE_DEMO_DUCKDB_PATH` defaults to a project-relative
DuckDB file, created automatically the moment Dagster's definitions load.

## Integration mapping — every system, its real component

| System | Component | Rung | Escalation reasoning |
|---|---|---|---|
| **Databricks** (PySpark jobs, battery_materials + catalysis/AC) | `dagster_databricks.DatabricksWorkspaceComponent`, subclassed as `DemoDatabricksWorkspaceComponent` | 1 (native), subclassed per rung 3 | Native package covers job discovery + `assets_by_job_task_key` mapping directly. Subclass adds the demo-mode discovery/execution seam **and** an observation sensor the native component lacks entirely — see `component-feedback/2026-09-12-databricks-workspace-observation-sensor.md`. |
| **Azure Data Factory** (Stonebranch-triggered legacy pipeline) | `dagster_community_components.AzureDataFactoryComponent`, subclassed as `DemoAzureDataFactoryComponent` | 2 (registry), subclassed per rung 3 | `dagster-component search "azure data factory" --json` → `azure_data_factory` directly (top hit). Same demo-mode monkeypatch seam and known sensor/key-override gaps first documented for demos/rvu-tempcover — reused, not re-derived. |
| **Power BI** (reporting, "Data Mesh Health" dashboard) | `dagster_powerbi.PowerBIWorkspaceComponent`, subclassed as `UmicorePowerBIComponent` | 1 (native), subclassed per rung 3 | `dagster-component search "power bi" --json` → zero registry hits, confirming native is correct. Demo-mode discovery + refresh seam, same shape as demos/rvu-tempcover's Power BI subclass. |
| **dbt Core** (transformation, real and currently running) | `dagster_dbt.DbtProjectComponent`, subclassed as `UmicoreDbtComponent` | 1 (native), subclassed for kind-badge only | Runs for real in both demo and live mode. Subclass only overrides `get_asset_spec` to badge `dbt`+`databricks` instead of the manifest-derived `duckdb`. |
| **Stonebranch** (upstream trigger, infra scheduling) | none — deliberately not integrated | n/a | Stonebranch is the *upstream trigger* under the coexistence pattern, never a system Dagster calls into. Represented as what it triggers (the ADF pipeline), observed via that pipeline's sensor. `dagster-component search "stonebranch" --json` returns nothing — expected, not a gap. |
| **Recycling/specialty_materials ingestion** (no tool named) | none — no kind badge beyond `azure` | n/a | AE notes mark per-BU ingestion "unknown." Built generic (`WarehouseTableAssetsComponent`, plain stub rows) rather than guessed specific-and-wrong, badged for the confirmed Azure cloud only. |
| **Warehouse/lakehouse product** (never named) | DuckDB in demo mode; `dbt-databricks` `live` target in `profiles.yml` | n/a | AE notes never confirm Synapse vs. Databricks Lakehouse. `profiles.yml`'s `live` target is a representative dbt-databricks profile, not a confirmed one — **flagged as an assumption**. |

## Fidelity: graph-first, dbt runs for real

Per the brief, every hand-written business-unit and rollup asset body is
graph-first (stubbed): deterministic, seeded placeholder rows, not a
synthetic-data-generation apparatus, and no real Umicore volumes are
guessed. **dbt is the exception, by design** (Axis 1 requires real I/O
regardless of Axis 2 fidelity) — the whole transformation layer runs a
real, small dbt Core project (native `dagster-dbt`) against DuckDB, with
real `not_null`/`unique` tests surfaced as Dagster asset checks
automatically.

The raw layer's stub rows still land as real rows in the shared DuckDB
warehouse (via the Databricks component's demo-mode execution seam, or
directly for the two generic-stub business units) — Axis 1 requires the
blocking checks and dbt sources to have something real to query, even
though Axis 2 fidelity is graph-first.

Flipping to Umicore's real Databricks workspace is a `demo_mode: false` +
real `host`/`token` change in `defs/databricks_workspace/defs.yaml` (same
asset keys, specs, partitions, and YAML schema either way —
`templates/demo_mode_pattern.py`). Same pattern for ADF
(`defs/legacy_orchestration/defs.yaml`) and Power BI
(`defs/reporting/defs.yaml`). Flipping dbt to a real warehouse is a
`dbt_project/profiles.yml` target change.

## The asset graph

Fourteen assets across six groups, one lineage graph:

- **`battery_materials`** (2 assets) — `raw_cathode_production_batch`
  (badged `databricks`, real `DatabricksWorkspaceComponent` job trigger)
  and `marts/fct_battery_carbon_footprint_inputs` (dbt mart, feeds the EU
  Battery Regulation carbon-footprint declaration; blocking completeness
  check).
- **`catalysis`** (1 asset) — `raw_ac_daily_feed` (badged `databricks`,
  the AC/Automotive Catalysts feed named explicitly in the pain example;
  freshness policy; warning quality check standing in for the AC team's
  daily manual log check).
- **`recycling`** (1 asset) — `raw_recycling_batch_yield` (badged `azure`;
  `deps:` on the Stonebranch-triggered ADF pipeline — the one point in the
  graph where the legacy-scheduled and Dagster-owned sides visibly meet).
- **`specialty_materials`** (1 asset) — `raw_specialty_materials_output`
  (badged `azure`; the first job in Anthony's 3-job sequential chain;
  blocking arrival check gates everything downstream).
- **`dbt`** (4 assets, badged `dbt`+`databricks`) — the staging layer:
  `staging/stg_battery_materials`, `staging/stg_catalysis_ac`,
  `staging/stg_recycling`, `staging/stg_specialty_materials`.
- **`corporate_rollup`** (4 assets) — `marts/fct_cross_bu_consolidated_ledger`
  (the untangled spiderweb: one consolidated ledger across all four BUs;
  freshness policy; chain stage 2, eager), `marts/executive_reporting_extract`
  (chain stage 3, eager — the money shot's final asset),
  `marts/eu_battery_regulation_carbon_footprint_report` (declaration-facing
  extract, no footprint math per house rules), and
  `power_bi_mesh_health_dashboard_refresh` (badged `powerbi`, the
  dashboard where staleness is currently first noticed).
- **`legacy_orchestration`** (1 asset) — `adf_pipeline_stonebranch_nightly_mesh_sync`,
  observed via its own polling sensor.

**Note on asset count:** the brief's Build directives specify "roughly
14-18 assets: 2-3 per business unit plus 3-4 corporate rollup assets,"
sized toward the higher end since a data-mesh/platform-consolidation story
should read as comprehensive. This build lands at 14 (2-3 per BU + 4
corporate rollup + 1 legacy), within range without padding for tidiness.

**A note on asset execution vs. automation.** `adf_pipeline_stonebranch_nightly_mesh_sync`
is, like any real `AzureDataFactoryComponent` pipeline, technically
materializable (a person could click Materialize in the UI, which would
trigger the real ADF pipeline in production or the demo-mode fake here).
But nothing in this build's schedules, sensors, or automation conditions
ever targets it — operationally, Dagster observes it and never triggers
it, matching the brief's "observed — never triggered" directive. This is
the same constraint demos/rvu-tempcover's ADF build operated under; there
is no config on the registry component to make a pipeline object
non-executable.

## Asset checks

| Check | Asset | Severity | Catches |
|---|---|---|---|
| `raw_specialty_materials_output_arrival` | `specialty_materials/raw_specialty_materials_output` | **Blocking** | Fewer than 20 rows on file for the day — Anthony's own example, the literal fix for "the other two sit stale until the next scheduled window." |
| `fct_battery_carbon_footprint_inputs_completeness` | `marts/fct_battery_carbon_footprint_inputs` | **Blocking** | Null/non-positive daily production totals — blocks the EU Battery Regulation carbon-footprint report from computing on incomplete inputs. |
| `raw_ac_daily_feed_quality` | `catalysis/raw_ac_daily_feed` | Warning | Low row count or column drift — the automated stand-in for the AC team's ~2-hours-a-day manual log check, without implying the feed was silently corrupt before. |
| 22 dbt-native `not_null`/`unique` tests | all 4 staging + all 4 mart models | Mixed (dbt default) | Missing/duplicate keys — auto-surfaced as Dagster asset checks, zero extra Dagster code. |

All checks compute genuine results against the stub/dbt data (not hardcoded
passes) — `validate_e2e.py` asserts they evaluate to `True`, not just that
they ran. Per house rules, everything stays green: no planted failure
anywhere in this build.

## Freshness, automation, and schedule

- **Freshness policies**: `marts/fct_cross_bu_consolidated_ledger` (the
  corporate rollup) and `catalysis/raw_ac_daily_feed` (fail at 24h, warn at
  18h) — the two assets named explicitly in the brief.
- **Automation conditions**: `AutomationCondition.eager()` on every dbt
  asset; `staging/stg_specialty_materials` additionally gates on
  `all_deps_blocking_checks_passed()` — the direct fix for the 3-job
  sequential chain, and the money-shot mechanism.
- **Schedule**: `umicore_morning_raw_layer_schedule` materializes all four
  business-unit raw data products at 05:00 Europe/Brussels — the one fixed
  trigger in the graph, at the genuine data-arrival boundary. Everything
  downstream recomputes via eager automation instead of its own fixed
  window — the direct contrast with "time-based, not event-based"
  scheduling. **Assumption**: no cutover time is named in the brief.
- **Retry policies**: none. Per house rules, a decorative retry on a
  deterministic stub source invites a question we'd lose; nothing here is
  genuinely flaky.
- **Observation sensors**: `legacy_orchestration_observation_sensor` (ADF,
  default `True` on the real component) and
  `databricks_workspace_observation_sensor` (opt-in `generate_sensor: true`,
  added in the subclass — the base component has no such field, see
  `component-feedback/`). Both detect runs Dagster didn't trigger.

## Three buckets

1. **Implemented in code**: the fourteen-asset graph across four business
   units plus corporate rollup; the Stonebranch/ADF-triggered external
   pipeline plus its observation sensor (the coexistence proof point); the
   Databricks-orchestrated battery_materials/catalysis raw layer plus its
   (newly added) observation sensor; three custom asset checks plus 22
   dbt-native tests; two freshness policies; eager automation on the
   3-job chain; one morning schedule; the Power BI refresh as a downstream
   observed asset.
2. **Handled by Dagster+, demonstrated not built**: **native alerting**
   (Slack/Teams) on check/freshness failures — the direct answer to "no
   central log or alert" and "at least one team down on any given day."
   **Restart-from-failure** — the direct answer to "root-causing means
   manually checking logs across ADF, PySpark, and Databricks separately."
   Asset lineage visualization — the literal "untangled spiderweb." Run
   history and duration trends.
3. **Conversation only, nothing built**: any change to Umicore's
   Stonebranch/Ansible infrastructure-scheduling footprint (explicitly out
   of scope — coexistence, not replacement); the EU Battery Passport/
   carbon-footprint calculation logic itself (Dagster's role is upstream
   data reliability, not the calculation); any data-catalog integration
   (none named); a fifth business unit (the business case's own "5
   business units" ROI-scaling row names no fifth unit — not invented
   here).

## `defs/` file count

**6 YAML, 7 Python** (plus boilerplate `defs/__init__.py`). **All 14 assets
are YAML-instantiated** — zero hand-written Python asset bodies anywhere in
`defs/`. Every Python file is justified support code:

- `defs/databricks_workspace/defs.yaml` — 2 assets (battery_materials,
  catalysis/AC), one `DemoDatabricksWorkspaceComponent` instance with an
  explicit `assets_by_job_task_key` mapping table.
- `defs/generic_raw_layer/defs.yaml` — 2 assets (recycling,
  specialty_materials), one `WarehouseTableAssetsComponent` instance.
- `defs/legacy_orchestration/defs.yaml`, `defs/reporting/defs.yaml` — the
  ADF and Power BI components, one instance each.
- `defs/transformation/staging/defs.yaml`, `defs/transformation/marts/defs.yaml`
  — the whole dbt layer (8 assets), `UmicoreDbtComponent` instantiated
  twice (unpartitioned staging vs. daily-partitioned marts).
- `defs/checks/*.py` (3 files) — **justified**: asset-check assertion
  logic is business logic; no registry component covers a declarative
  completeness/arrival/quality check against an arbitrary DuckDB query.
- `defs/automation/morning_raw_layer_schedule.py` — **justified**: the
  registry's `cron_schedule` component's partitioned-job mode can't
  express a specific hour alongside a `partitions_def`, so the
  one-function native `build_schedule_from_partitioned_job` call is used
  directly (same registry gap recorded for demos/rvu-tempcover,
  demos/partners-fcu).
- `defs/databricks_workspace/template_vars.py`,
  `defs/transformation/staging/template_vars.py`,
  `defs/transformation/marts/template_vars.py` — **justified**: Jinja
  (YAML template expressions) has no `&` operator and `dg`'s template
  scope doesn't expose `AutomationCondition`/`FreshnessPolicy` composition
  directly; exposing the composed Python values as `@dg.template_var`s is
  the documented sanctioned pattern.

`components/*.py` and `demo_data/*.py` are components/support code, not
`defs/` — only `defs/` is measured for the YAML-first ratio.

## Registry search record

Per CLAUDE.md's escalation ladder (at least three searches before writing
custom code) and the brief's named search terms:

```
dagster-component search "azure data factory" --json     → azure_data_factory (top hit, used)
dagster-component search "databricks workspace" --json   → databricks_resource, talend_cloud_workspace, databricks_asset_bundle (none cover job-run observation)
dagster-component search "power bi" --json                → zero hits (confirms native dagster-powerbi is correct)
dagster-component search "dbt project" --json              → enriched_dbt_project (native dagster-dbt used instead — already covers everything needed)
dagster-component search "databricks observation sensor"  → databricks_table_observation_sensor (table freshness, not job-run history — not a fit)
dagster-component search "databricks polling"             → informatica_workspace, ssis_workspace, talend_cloud_workspace (none relevant)
```

No custom (rung 4) component was written for this build — the one gap
found (Databricks job-run observation) was closed by subclassing the
native rung-1 component, documented in
`component-feedback/2026-09-12-databricks-workspace-observation-sensor.md`.

## Which parts run where

The dbt + raw layer's DuckDB file is local — it exists for the lifetime of
one process (or one Dagster+ Serverless container; **Serverless storage is
ephemeral**, so a fresh container starts with an empty warehouse each run).
**Give the interactive demo locally with `dg dev`** — the money-shot
eager-automation cascade and the multi-day partition history only persist
across materializations there. Treat the deployed Dagster+ code location as
proof the project is real and loads correctly, not as a place to run the
full chain live.

## Assumptions

- **No meeting is booked** for this brief — built from an internal AE
  business-case document, not a calendar invite. Treat as an undated,
  doc-only candidate.
- **Warehouse/lakehouse product** — never named; `profiles.yml`'s `live`
  target assumes dbt-databricks, not confirmed.
- **Schedule hour (05:00 Europe/Brussels)** and **freshness thresholds
  (24h fail / 18h warn)** — no SLA or timezone is named in the brief;
  Belgium HQ is the most plausible default.
- **Row-count floors** (20 for specialty-materials arrival, 20 for AC feed
  quality) and **stub cardinalities** (60 rows/day per business unit) — no
  real Umicore volumes are given; small, deterministic, clearly-synthetic
  numbers per house rules.
- **Recycling's raw ingestion mechanism** — the AE notes mark per-BU
  ingestion "unknown"; this build models it as landed by the
  Stonebranch/ADF pipeline specifically (rather than a wholly separate
  generic source) so the graph has a concrete point where the legacy and
  Dagster-owned sides visibly meet, per the brief's most load-bearing
  directive.
- **No fifth business unit** — the business case's own ROI table scales to
  "5 business units" without naming a fifth; built against the four named
  units only, per the brief's explicit instruction.
- **No planted failure, no Stonebranch/Ansible replacement modeling** —
  explicitly out of scope per the brief; the graph stays green throughout.
