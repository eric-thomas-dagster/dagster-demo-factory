# LEARNINGS

Verified facts from previous Demo Factory runs. Read after `CLAUDE.md`, before
building. Every run updates it (step 10 of the Demo Factory prompt).

**Facts, not rules.** How a build *should* behave belongs in `CLAUDE.md`. This
file is for things that are true about the tooling — command forms, schemas,
version quirks, dead ends. If an entry reads like a policy, move it.

**Maintenance contract — every run does all three, in this order:**

1. **Invalidate.** If an entry is now wrong, fix or delete it. A stale entry is
   worse than none, because the next run trusts it.
2. **Prune.** If a gap you recorded has since been closed, or an entry no longer
   earns its context cost, delete it.
3. **Append.** Only then add what this run verified — by running it or reading
   source. Never speculation.

One or two lines each. No narrative, no run stories. Version-sensitive facts
carry the version they were checked against, so a future run knows when to
re-verify. Hard cap ~100 lines.

---

## Deployment packaging — extra gotchas beyond preflight_deploy.sh's own checks

- `--package-name`/`-p` must be the module holding `Definitions`; verify with
  `python -c "import <pkg>"` first. `--module-name`/`-m
  <pkg>.definitions` also works when `Definitions` is built by a
  `@dg.definitions`-decorated function rather than a bare module attr.
- `deploy_demo.sh` must activate the project venv first, or deploy dies with
  `dagster-cloud: command not found` after validation passed.
- `.gitignore`d files don't ship — dbt `target/manifest.json` and defs-state
  are usually gitignored; force-include via `[tool.hatch.build.targets.wheel]`.

## Dagster+ Serverless runtime

- Storage is **ephemeral** — each run is a fresh container; local DuckDB files
  don't persist between runs. Anything spanning multiple runs works locally and
  silently breaks in Serverless. Give interactive demos via `dg dev`; treat the
  Dagster+ deployment as proof the project loads.

## CLI

- **Use `dg`, never the legacy `dagster` CLI.** `dg dev` not `dagster dev`;
  `dg launch --assets '*'` not `dagster asset materialize --select '*'`. The
  flag differs too: `--assets` vs `--select`. (dagster-dg-cli 1.13.19)
- `dg launch` options: `--assets`, `--job`, `--partition`,
  `--partition-range <start>...<end>`, `--config` / `--config-json`.
- **`dg launch --assets '*'` cannot validate a partitioned project** — it exits
  with "Asset has partitions, but no '--partition' option was provided". Every
  build must ship `validate_e2e.py`; `scripts/validate_demo.sh` calls it.
- `Definitions.resolve_implicit_job_def_def_for_assets(asset_keys)` is the real
  method name — the doubled `def_def` is not a typo. Delegates to
  `get_repository_def().get_implicit_job_def_for_assets()`. (dagster, verified
  2026-08-24)
- `dagster-component init` does **not** scaffold a project — it writes AI-tool
  config and wires the `registry_modules` entry point into an existing one. Run
  `create-dagster project` first.
- Pass `--auto-install` to `dagster-component init` / `add`, or they prompt and
  hang forever unattended.
- The `dagster-community-components-cli` README is stale; the package also has
  `init`, `sync-deps`, `analyze-schedules`. Check `--help`. (0.8.15)
- If `dg list components` misses custom components, re-run
  `dagster-component init --force`.

## APIs and schemas

- dbt asset `kinds` derive from the manifest's `adapter_type`, so DuckDB badges
  every model `duckdb`. No `get_kinds` hook — subclass `DagsterDbtTranslator`,
  override `get_asset_spec(self, manifest, unique_id, project) -> dg.AssetSpec`,
  and `spec.replace_attributes(kinds={"dbt", "snowflake"})`. (dagster-dbt,
  verified 2026-08-24)
- Assets and `AssetSpec`s accept `kinds={"snowflake"}` directly. Max 3 per asset.
- `components/__init__.py` must re-export each component class, or the UI
  Components tab won't list them even when `dg list components` does.
- Non-dbt state-backing (e.g. a resource-only registry component) still needs
  `dg utils refresh-defs-state` before deploying if it's a state-backed
  component — check with `dagster-component info <id>` for a `defs_state`
  field, not just "does this project use dbt/Fivetran."
- Current (non-legacy) freshness API: `dg.FreshnessPolicy.cron(deadline_cron=,
  lower_bound_delta=, timezone=)` passed to `@asset(freshness_policy=...)`.
  `dg.LegacyFreshnessPolicy` still exists but isn't the one to reach for.
  (dagster 1.13.19)
- `dg.build_schedule_from_partitioned_job` errors if you pass
  `cron_schedule`/`execution_timezone` together with
  `hour_of_day`/`minute_of_hour`, and errors again if you pass either at all
  for a time-partitioned job — set `timezone=` on the
  `DailyPartitionsDefinition(...)` itself instead. (dagster 1.13.19)
- `profiles.yml` (dbt) needs a **working default path** with the env var as an
  optional override: `{{ env_var('X_DUCKDB_PATH', 'demo_data/demo.duckdb') }}`.
  Requiring it with no fallback ships a demo that won't start.

## Partitions — MultiPartitionsDefinition

- `MultiPartitionKey`'s string form (for `--partition` /
  `execute_in_process`) orders dimensions **alphabetically by dimension
  name**, not declaration order — don't hand-construct the `"a|b"` string,
  build it directly: `dg.MultiPartitionKey({"date": d, "dealer_group": g})`.
- `dg.MultiToSingleDimensionPartitionMapping(partition_dimension_name="date")`
  on a downstream date-only asset's `AssetDep` maps it to *every* value of the
  upstream's other dimension for that date — use when a downstream asset
  rolls up a dimension (e.g. region) only the upstream carries.
- One `dg.define_asset_job` (and `build_schedule_from_partitioned_job`)
  requires every selected asset to share one `partitions_def`. A
  multi-partitioned asset needs its own separate job/schedule from
  date-only assets in the same layer.

## Environment

- `GH_TOKEN` reads as literal `proxy-injected` when the GitHub proxy handles
  auth. Not a usable token — treat as unset.
- Gmail exposes `create_draft` but no send, and routines run without approval
  prompts. Draft + mobile push; never report the missing send as a failure.
- Cloud env vars are **not** visible to the setup script — session shell only.
- Briefs and `state/ledger.json` must live on `main`; anything on an unmerged
  branch is invisible to Factory and the run silently no-ops.

## Registry coverage worth knowing

- Microsoft Fabric is covered: `fabric_workspace`,
  `fabric_pipeline_trigger_job`, `fabric_lakehouse_resource`,
  `fabric_lakehouse_io_manager`, `dataframe_to_fabric_lakehouse`,
  `fabric_capacity_admin_job`, `fabric_workspace_resource`. Plus ~66 Azure and
  ~18 Databricks components, and `azure_synapse` / `synapse_sql_pool_admin_job`.
  Search before building anything Fabric or Azure from scratch. (2026-08-26)
- **None of the Fabric components fit a named, partitioned, checked asset
  graph directly.** `fabric_workspace` discovers whatever items exist in a
  live workspace as *unpartitioned* assets; `fabric_pipeline_trigger_job`
  produces a job+schedule, not a lineage-graph asset. For a Fabric
  trigger-and-observe demo with your own asset names/partitions/checks, wire
  `fabric_workspace_resource` (`FabricWorkspaceResource`:
  `list_items`/`trigger_item_run`/`wait_for_run`) as a resource via
  defs.yaml, then write your own `@asset` functions that inject it — still
  rung 2, just not a registry-provided asset shape. (2026-08-26)

## Dead ends

- **Never model recovery as an action inside Dagster.** No heal asset, no heal
  job, no reset object. Assets are idempotent — model late data as source
  arrival timing in the mock so a plain rematerialize is the whole story. A
  disconnected `healed_partitions` node reads as scaffolding.
