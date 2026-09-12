# Databricks workspace component has no job-run observation sensor

**Date:** 2026-09-12
**Build:** demos/umicore ("Untangling the Spiderweb")

## What was needed

Umicore's brief requires every external-system component to observe as well
as execute (house rule, "Observe, don't just execute") -- specifically, a
polling sensor that detects Databricks job runs Dagster didn't trigger
(e.g. a data scientist re-running a notebook manually, or another
scheduler firing the job) and emits `AssetObservation` events for them,
matching the convention already shared by `AzureDataFactoryComponent`
(`polling_sensor`, default `True`) and `PowerBIWorkspaceComponent`'s
semantic-model refresh path.

`dagster_databricks.DatabricksWorkspaceComponent` (native package, rung 1
of the escalation ladder -- not a registry component) covers job discovery
(`assets_by_job_task_key`, `write_state_to_path` via
`DatabricksWorkspace.fetch_jobs()`) and execution
(`_create_job_asset_def`/`run_now`+poll) well, following the same
`StateBackedComponent` + explicit-mapping conventions as the other
workspace-style components. But it has **no observation surface at all**.

## What was searched

Three distinct registry searches, all with `--json`:

```
dagster-component search "databricks workspace" --json
dagster-component search "databricks observation sensor" --json
dagster-component search "databricks polling" --json
```

Results:
- `"databricks workspace"` → `databricks_resource` (188), `talend_cloud_workspace`
  (185), `databricks_asset_bundle` (183) -- none address job-run observation.
- `"databricks observation sensor"` → `databricks_table_observation_sensor`
  (393, top hit), `snowflake_workspace` (30), `ssis_workspace` (25).
- `"databricks polling"` → `informatica_workspace` (35), `ssis_workspace` (20),
  `talend_cloud_workspace` (15).

## What came closest, and why it didn't fit

`databricks_table_observation_sensor` scored highest and looked promising by
name, but `dagster-component info databricks_table_observation_sensor`
shows it "emit[s] health observations for an external Databricks Delta
table" -- it polls a *table's* freshness/row-count/schema state, not a
*job's* run history. It has no notion of a Databricks job, a job run, or a
run's trigger source. It cannot answer "did someone run this job outside
Dagster" at all; it answers a different question ("is this table's data
current"). Not a fit for job-run observation, only tangentially adjacent by
keyword.

No other search result names Databricks jobs/job-runs at all. Confirmed:
there is no registry component, and no field on the native
`DatabricksWorkspaceComponent` itself, that covers job-run observation.

## What was built instead

`demos/umicore/src/umicore/components/demo_databricks.py`:
`DemoDatabricksWorkspaceComponent(DatabricksWorkspaceComponent)` adds a
`generate_sensor: bool = False` field (off by default, matching Power BI's
convention rather than ADF's -- see LEARNINGS.md, this varies per
component) and a `build_defs_from_state` override that appends a
`@dg.sensor`-built `SensorDefinition` when enabled. The sensor reads
`DemoDatabricksWorkspace.list_recent_runs(job_name)` -- demo mode returns a
fixed external-run fixture; real mode resolves the job id via
`client.jobs.list(name=...)` and calls `client.jobs.list_runs(job_id=...,
limit=25)` -- and emits one `AssetObservation` per unseen terminal run
against every asset key `assets_by_job_task_key` maps to that job. Every
other part of the parent component (discovery, execution, asset specs,
`assets_by_job_task_key` mapping) is untouched real code.

## Suggested change

Add the same `polling_sensor` (alias `generate_sensor`) + `poll_interval_seconds`
fields to `dagster_databricks.components.databricks_workspace.component.
DatabricksWorkspaceComponent` directly, with a sensor built from
`DatabricksWorkspace.get_client().jobs.list_runs(job_id=...)` per job in
`assets_by_job_task_key`, following exactly the same shape as
`AzureDataFactoryComponent`'s `polling_sensor`. This is a small, contained
addition (two config fields + one sensor-building method) that would let
every future Databricks-orchestrating demo skip this subclass entirely.
