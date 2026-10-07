# Skygen build: seams and gaps in Fivetran, Sigma, SSIS (and two systems with no component)

No custom component was written: every integration is a real component, used
as-is or subclassed for demo mode. These are the gaps found doing so.

## What was needed
Demo-mode (no credentials) versions of the Fivetran, Sigma and SSIS
integrations, tenant x date partitions on Fivetran assets, and observation
for each external system.

## What was searched (all with `--json`)
```
uvx --from dagster-community-components-cli dagster-component search "<term>" --json
```
Terms and top results:
- `fivetran` -> fivetran_assets, fivetran_sync_sensor, fivetran_sync_trigger_job (community); native `dagster_fivetran.FivetranAccountComponent` is the better fit (state-backed, polling sensor) -> used.
- `sigma` -> sigma_input_table_upsert, sigma_resource, sigma_workbook_ingestion (a DataFrame source, not lineage assets); native `dagster_sigma.SigmaComponent` used.
- `ssis` -> ssis_workspace (used, subclassed).
- `snowflake` -> resources/sinks only; no job-orchestration need. Snowflake is a kinds badge on dbt/Fivetran assets; dbt's `live` profile target is the real path.
- `multi tenant`, `terraform` -> nothing matching tenant fan-out; terraform_asset/terraform_cloud_asset exist but Terraform is conversation-only in this brief.
- `powershell`, `azure devops` -> **no results**. `github actions` -> only http_external_asset (generic). The PowerShell/CI-CD legacy tooling therefore has no component; it is represented by the SSIS package that wraps it and by Dagster+ branch deployments in the talk track.
- `asset check`, `row count check`, `freshness check` -> nothing for a partition-aware row-count-vs-source check; native `@multi_asset_check` used.

## What came closest, and why it needed a subclass
- `dagster_fivetran.FivetranAccountComponent`: the `workspace` field is hard-wired
  to `FivetranWorkspace` through its own resolver, so injecting a demo client meant re-declaring the
  field in a subclass. Suggested change: a `client`/`demo_mode` hook on the workspace, or resolving
  the class through an overridable attribute. No `partitions_def` field either; the shared
  `translation:` block covers it, which worked.
- `dagster_sigma.SigmaComponent`: no observation surface (nothing emits events when a workbook is
  refreshed outside Dagster). Suggested change: an opt-in polling sensor like the Fivetran/Power BI
  components. Not built here (cut for time; the workbooks are external specs).
- `ssis_workspace`: the observation sensor re-derives asset keys instead of calling
  `get_asset_spec`, so overriding keys splits sensor identity from asset identity (same gap as
  component-feedback/2026-09-24-ssis-workspace-gaps.md; worked around by rebuilding the sensor in
  `DemoSsisWorkspaceComponent`).

## What was built instead
Subclasses only: `src/skygen/components/demo_fivetran_account.py`, `demo_sigma.py`,
`demo_ssis_workspace.py`, `skygen_dbt_project.py`.
