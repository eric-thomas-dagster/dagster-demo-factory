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

**No tool-specific content and no registry inventories.** The registry has ~975
components covering thousands of tools; listing which ones exist would go stale
immediately and is what `dagster-component search --json` is for. Record
*conventions and behaviours* that reading the source reveals — a field's default,
a shared class shape, a documented override hook. Never "tool X has/lacks a
component," and never a spec for integrating a specific tool.

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
  hang.
- **Run the deploy in the FOREGROUND via `./scripts/deploy_demo.sh`** and let it
  block — it polls to a terminal state internally. Backgrounding it and
  managing the task separately just re-implements the wait the script already
  does. Sequence the deploy last, after the retro and notification draft.
- **Never hand-roll the `dagster-cloud` command.** The script carries the
  correct flags, the LOADED loop, and partial-failure cleanup.
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
- `components/__init__.py` must re-export each component class, or the UI
  Components tab won't list them even when `dg list components` does.

- **Declaring a bare `dg.AssetSpec` (not wrapped in `@asset`) in
  `Definitions(assets=[...])` is how you get a real external/observed-only
  asset** — no `external_asset_from_spec` function exists (checked `dir(dg)`,
  dagster==1.13.19). `Definitions._canonicalize_specs_to_assets_defs` wraps
  bare specs into an `AssetsDefinition(specs=specs)` with zero compute; its
  materialization history is then only ever `AssetObservation`/
  `AssetCheckEvaluation` events reported via
  `instance.report_runless_asset_event(event)` (exactly what a sensor's
  `SensorResult.asset_events` becomes when the daemon runs it — same call
  works standalone for testing). (verified 2026-08-27)
- **A checks-only job over a non-executable asset can't get a
  `PartitionsDefinition`, even if you pass one explicitly.**
  `AssetSelection.checks_for_assets(key)` + `define_asset_job(...,
  partitions_def=X)` silently drops `X` on resolution:
  `build_asset_job`'s `_infer_and_validate_common_partitions_def` only looks
  at `asset_graph.executable_asset_keys`, which is empty when the job selects
  checks on a bare-`AssetSpec` (external) asset, so it returns `None`
  regardless of what was passed in — `execute_in_process(partition_key=...)`
  then fails with "There is no PartitionsDefinition shared by all the
  provided assets". No fix at the YAML/config level; the check has to be
  evaluated directly (call the check function, wrap the result as an
  `AssetCheckEvaluation`, report it via `report_runless_asset_event`) instead
  of through a dedicated job. (dagster==1.13.19, confirmed by reading
  `_infer_and_validate_common_partitions_def` in
  `dagster/_core/definitions/assets/job/asset_job.py`; 2026-08-27)
- `SensorDefinition.evaluate_tick(context)` (context from
  `dg.build_sensor_context(instance=..., definitions=..., cursor=...)`)
  returns a `SensorExecutionData` with `.asset_events` and `.cursor` — the
  way to unit-test a sensor's logic without running the daemon. Cursor must
  be threaded manually between calls (`cursor=data.cursor` each tick) or the
  sensor never advances past its first rotation item. (verified 2026-08-27)

## Project config

- `profiles.yml` needs a **working default path** with the env var as an
  optional override: `{{ env_var('X_DUCKDB_PATH', 'demo_data/demo.duckdb') }}`.
  Requiring it with no fallback ships a demo that won't start.

## Environment

- `GH_TOKEN` reads as literal `proxy-injected` when the GitHub proxy handles
  auth. Not a usable token — treat as unset.
- Gmail exposes `create_draft` but no send, and routines run without approval
  prompts. Draft + mobile push; never report the missing send as a failure.
- Cloud env vars are **not** visible to the setup script — session shell only.
- Briefs and `state/ledger.json` must live on `main`; anything on an unmerged
  branch is invisible to Factory and the run silently no-ops.

## Registry behaviour and conventions

- **Never assert a registry gap without searching.** The registry includes
  thin wrappers over core Dagster calls (e.g. `cron_schedule`), so "this is
  core Dagster" is not evidence of absence. Search, always, with `--json`.
  (2026-08-26)
- Use the **workspace-style** component with an explicit mapping table in
  `defs.yaml`, not one instance per external object. Reference:
  `github.com/eric-thomas-dagster/databricks-workspace-bundles-demo`
  (`assets_by_task_key` in `defs/workspace_us/defs.yaml`). (2026-08-26)
- **Workspace components share one convention**: `@public` class,
  `translation:` field, `@public get_asset_spec(props)` override hook,
  `polling_sensor` (alias `generate_sensor`, **default False**), `defs_state` +
  `defs_state_config`, `StateBackedComponent` inheritance. Holds for
  `FabricWorkspaceComponent`, `FivetranAccountComponent`,
  `SnowflakeWorkspaceComponent`, `MLflowWorkspaceComponent`,
  `DatabricksWorkspaceComponent`, `PowerBIWorkspaceComponent`. (verified
  2026-08-26)
- **Observation sensors default to OFF.** Set `generate_sensor: true` or a demo
  only executes and never sees runs it didn't trigger. (2026-08-26)
- `StateBackedComponent` enumeration happens in the **state-write path**, so no
  HTTP fires at Dagster load time. "It queries a live connection at load time"
  is not a valid reason to reject one. (2026-08-26)

## Don't rebuild platform features

- Dagster+ has **native alert policies** for Slack, Teams, email, and
  PagerDuty, covering run failures, asset check failures, freshness violations,
  and schedule/sensor failures. Never hand-roll alerting in a demo — it implies
  the platform lacks something it has. Show it in the UI instead. (2026-08-25)

- Jobs: use `define_asset_job` with `AssetSelection`. Never call asset functions
  inside a job definition. (2026-08-26)

- **Verify each feature-floor item actually appears in `dg list defs --json`.**
  A component declaring a config field does not mean it builds anything from
  it. Confirm presence in the definitions listing; don't assume the component
  honoured its own config. (2026-08-26)
- Read the most recent successful project in `demos/` for established
  conventions (warehouse setup, check style, README shape) before inventing your
  own. Cheap, and it keeps builds consistent. (2026-08-26)
