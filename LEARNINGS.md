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

## Deployment packaging — check before deploying

`./scripts/preflight_deploy.sh <slug>` checks all of these in ~20s. Use it
instead of discovering them one deploy cycle at a time.

- `pex` must be installed, or `--build-method local` fails immediately.
- `dagster-cloud` must be a project dependency, not just a CLI on PATH.
- `--package-name` must be the module holding `Definitions`, not the project
  directory name. Verify with `python -c "import <pkg>"`.
- `dbt_project/` must live **inside** the Python package dir or it won't ship in
  the wheel; the location then fails to load with a confusing path error.
- `.gitignore`d files don't ship. dbt `target/manifest.json` and defs-state are
  usually gitignored — force-include via `[tool.hatch.build.targets.wheel]`.
- Run `dg utils refresh-defs-state` before deploying when using state-backed
  components (Fivetran, dbt), or the location fails to load remotely.
- Verify wheel *contents*, don't trust config: `python -m build` then
  `unzip -l dist/*.whl | grep -E "manifest|defs_state"`.
- `deploy_demo.sh` must activate the project venv first, or deploy dies with
  `dagster-cloud: command not found` after validation passed.

## Deployment — timing and confirmation

- Agent sync after PEX upload routinely takes several minutes. Normal, not a
  hang. Use a blocking wait on the background task; don't poll tightly.
- Exit code 0 from deploy does **not** mean the location loaded. Confirm with
  `dg api` and look for `LOADED`.
- `LOADED` means the definitions parsed. It does **not** mean assets
  materialize in the cloud.

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
- `dagster-component search` takes **one positional argument**. Multiple
  positional terms are rejected. Put several terms *inside* one quoted string —
  they're AND-ed across id, name, description, tags, keywords, agent_hints:
  `dagster-component search "fabric pipeline asset" --json`. `--json` returns
  `{id, score, matched_fields, matched_terms, category, produces, description}`.
  (verified 2026-08-26)
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

## Project config

- `profiles.yml` needs a **working default path** with the env var as an
  optional override: `{{ env_var('X_DUCKDB_PATH', 'demo_data/demo.duckdb') }}`.
  Requiring it with no fallback ships a demo that won't start.

## Build sequencing that works

- Run `dg check defs` after each layer (ingestion → SaaS → dbt), not once at the
  end. Failures localize instead of compounding.

## Connector quirks

- **Drive search only returns Google-native files.** Docs/Sheets/Slides are
  searchable; PDFs, DOCX, XLSX, PPTX are not, even though Drive indexes them.
  Use the recent-files listing (returns metadata for all types) alongside
  search, then read the file directly — *reading* PDFs and Office files works,
  only search is blind to them. (2026-08-26)
- Shared Drive content sometimes returns empty and looks like a permissions
  error but isn't. If a Shared Drive comes back blank, ask for the file by
  exact name or URL. (2026-08-26)

## Environment

- `GH_TOKEN` reads as literal `proxy-injected` when the GitHub proxy handles
  auth. Not a usable token — treat as unset.
- Gmail exposes `create_draft` but no send, and routines run without approval
  prompts. Draft + mobile push; never report the missing send as a failure.
- Cloud env vars are **not** visible to the setup script — session shell only.
- Briefs and `state/ledger.json` must live on `main`; anything on an unmerged
  branch is invisible to Factory and the run silently no-ops.

## Registry components that already exist (search before writing)

- Scheduling/automation: `cron_schedule` ("run an asset selection on a cron
  schedule"), `interval_schedule`, `automation_condition_applicator`,
  `per_partition_backfill_job`. **A previous run wrote a custom cron-schedule
  component claiming "nothing to search the registry for" — it exists.** Never
  assert a gap without running the search. (2026-08-26)
- Microsoft Fabric: `fabric_workspace`, `fabric_pipeline_trigger_job`,
  `fabric_lakehouse_resource`, `fabric_lakehouse_io_manager`,
  `dataframe_to_fabric_lakehouse`, `fabric_capacity_admin_job`. Plus ~66 Azure,
  ~18 Databricks, and `azure_synapse` / `synapse_sql_pool_admin_job`.
  (2026-08-25)
- Use the **workspace-style** component with an explicit mapping table in
  `defs.yaml`, not one instance per external object. Reference:
  `github.com/eric-thomas-dagster/databricks-workspace-bundles-demo`
  (`assets_by_task_key` in `defs/workspace_us/defs.yaml`). (2026-08-26)
- `fabric_workspace` discovering items from a live connection is **not** a
  reason to reject it — subclass and mock the discovery in demo mode, and add
  an explicit `assets_by_item_name`-style mapping, exactly as the Databricks
  demo does. A 2026-08-26 run rejected it on those grounds and wrote a custom
  component instead, skipping the subclass rung. (2026-08-26)

## Don't rebuild platform features

- Dagster+ has **native alert policies** for Slack, Teams, email, and
  PagerDuty, covering run failures, asset check failures, freshness violations,
  and schedule/sensor failures. Never hand-roll alerting in a demo — it implies
  the platform lacks something it has. Show it in the UI instead. (2026-08-25)

## Dead ends

- **Never plant a failure in a demo.** No anomalies, corrupt partitions, or
  missing data — not behind a flag. A demo that can fail will fail live, on the
  path nobody rehearsed. Build the checks and explain what they'd catch in
  production, against a green graph. Corollary: nothing to heal, so no heal
  asset, heal job, or reset object; a disconnected `healed_partitions` node
  reads as scaffolding. Briefs cannot override this. (2026-08-25)
