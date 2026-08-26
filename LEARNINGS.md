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

## Deployment

`./scripts/preflight_deploy.sh <slug> [pkg]` checks packaging in ~20s — use
it instead of discovering these one deploy cycle at a time. `pkg` defaults
to `slug`; pass it explicitly when the module name uses underscores where
the slug uses hyphens, or preflight fails on step 3 for the wrong reason.

- `pex` installed, `dagster-cloud` a real `pyproject.toml` dependency (not
  just a CLI on PATH), `--package-name` the module holding `Definitions`
  (verify with `python -c "import <pkg>"`).
- `.gitignore`d files don't ship (dbt `target/manifest.json`, defs-state) —
  force-include via `[tool.hatch.build.targets.wheel]`. `dbt_project/` must
  live inside the package dir. Run `dg utils refresh-defs-state` before
  deploying any state-backed component (Fivetran, dbt).
- `deploy_demo.sh` must activate the project venv first, or it dies with
  `dagster-cloud: command not found` after validation passed.
- Agent sync after PEX upload takes several minutes — normal, use a blocking
  wait, don't poll tightly. Exit code 0 does **not** mean the location
  loaded — confirm with `dg api`, look for `LOADED` (which itself only means
  definitions parsed, not that assets materialize).
- Prefer `deploy-python-executable --build-method local` (PEX);
  `serverless deploy` (Docker) is the fallback for source-only deps.
- Dagster+ Serverless storage is **ephemeral** — each run is a fresh
  container, so local DuckDB files don't persist between runs there. Give
  interactive demos via `dg dev`; treat the Dagster+ deployment as proof the
  project loads, not a place to run multi-run sequences.

## CLI

- **`dg launch --assets '*'` cannot validate a partitioned project** — it exits
  with "Asset has partitions, but no '--partition' option was provided". Every
  build must ship `validate_e2e.py`; `scripts/validate_demo.sh` calls it.
- `Definitions.resolve_implicit_job_def_def_for_assets(asset_keys)` is the real
  method name — the doubled `def_def` is not a typo. Delegates to
  `get_repository_def().get_implicit_job_def_for_assets()`. (dagster, verified
  2026-08-24)
- `dagster-component init` does **not** scaffold a project — it writes AI-tool
  config and wires the `registry_modules` entry point into an existing one. Run
  `create-dagster project` first, then `init --auto-install --force` (or it
  prompts and hangs forever unattended).
- `dagster-component search` takes **one QUERY string**, not multiple CLI
  args — put every term inside that one string for AND matching:
  `dagster-component search "sql server ssis" --json`. Separate args
  (`search "sql server" ssis`) errors "Got unexpected extra arguments".
  (dagster-community-components-cli, corrected 2026-08-26 — an earlier
  entry here had this backwards)
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
- `dg.AssetCheckExecutionContext` exposes `has_partition_key`/`partition_key`
  (not `has_asset_partitions`) — same attribute names as
  `AssetExecutionContext`. (dagster 1.13.19)
- `dg.build_schedule_from_partitioned_job` **rejects `cron_schedule`/
  `execution_timezone` for a time-partitioned job** (a plain
  `DailyPartitionsDefinition` job, or a `MultiPartitionsDefinition` job with
  one time dimension) — pass `hour_of_day`/`minute_of_hour` instead; the
  timezone comes from the `partitions_def` itself. (dagster 1.13.19)
- A registry component's `add`-time `uv pip install` isn't recorded in
  `pyproject.toml` — a later plain `uv sync` silently **uninstalls** those
  packages. Add real runtime deps it needs (e.g. `azure-identity`,
  `requests` for a Fabric/Azure component) to `pyproject.toml`
  `dependencies` yourself, not just via the CLI's auto-install.

## Environment

- `GH_TOKEN` reads as literal `proxy-injected` when the GitHub proxy handles
  auth. Not a usable token — treat as unset.
- Gmail exposes `create_draft` but no send, and routines run without approval
  prompts. Draft + mobile push; never report the missing send as a failure.
- Cloud env vars are **not** visible to the setup script — session shell only.
- Briefs and `state/ledger.json` must live on `main`; anything on an unmerged
  branch is invisible to Factory and the run silently no-ops.

## Registry coverage worth knowing

- Microsoft Fabric: `fabric_workspace` (imports items from a *live* workspace
  as unpartitioned assets), `fabric_pipeline_trigger_job` (bare job+schedule,
  not an asset), `fabric_workspace_resource` (REST client:
  `list_items`/`trigger_item_run`/`wait_for_run`), `fabric_lakehouse_resource`,
  `fabric_lakehouse_io_manager`, `dataframe_to_fabric_lakehouse`,
  `fabric_capacity_admin_job`. Plus ~66 Azure and ~18 Databricks components.
  **None of these produce a named/partitioned/checked asset** for a
  trigger-and-observe demo — wire `fabric_workspace_resource` as a resource
  and write one custom asset-producing component around it (still rung 2:
  registry resource, as-is; the asset component itself is rung 4 — write a
  `component-feedback/` entry). (re-verified 2026-08-26)
