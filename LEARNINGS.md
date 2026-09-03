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
  hang. Run `./scripts/deploy_demo.sh` in the **foreground** and let it block
  (it polls to a terminal state internally) rather than backgrounding it and
  managing a separate monitor/wakeup loop. Sequence deploy last, after the
  retro and notification draft.
- **Never hand-roll the `dagster-cloud` command** — the script carries the
  correct flags, the LOADED loop, and partial-failure cleanup.
- Exit code 0 from deploy does **not** mean the location loaded (`LOADED`
  means definitions parsed, not that assets materialize). Confirm with
  `dg api`.

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

- **Never assert a registry gap without searching.** A 2026-08-26 run wrote a
  custom cron-schedule component stating "nothing to search the registry for" —
  `cron_schedule` exists. The registry includes thin wrappers over core Dagster
  calls, so "this is core Dagster" is not evidence of absence. Search, always,
  with `--json`. (2026-08-26)
- Use the **workspace-style** component with an explicit mapping table in
  `defs.yaml`, not one instance per external object. Reference:
  `github.com/eric-thomas-dagster/databricks-workspace-bundles-demo`
  (`assets_by_task_key` in `defs/workspace_us/defs.yaml`). (2026-08-26)
- **Workspace components share one convention**: `@public` class,
  `translation:` field, `@public get_asset_spec(props)` override hook,
  `polling_sensor` (alias `generate_sensor`), `defs_state` +
  `defs_state_config`, `StateBackedComponent` inheritance. Holds for
  `FabricWorkspaceComponent`, `FivetranAccountComponent`,
  `SnowflakeWorkspaceComponent`, `MLflowWorkspaceComponent`,
  `DatabricksWorkspaceComponent`, `PowerBIWorkspaceComponent`,
  `AzureDataFactoryComponent`. (verified 2026-08-26, 2026-09-03)
- **Observation sensors usually default to OFF** (`polling_sensor`/
  `generate_sensor: false`) — check every workspace component's actual
  default before assuming, though: `AzureDataFactoryComponent` is a
  confirmed exception, defaulting **True**. Read the field, don't assume
  the convention. (2026-08-26, corrected 2026-09-03)
- `StateBackedComponent` enumeration happens in the **state-write path**, so no
  HTTP fires at Dagster load time. "It queries a live connection at load time"
  is not a valid reason to reject one. (2026-08-26)
- **Not every workspace component exposes an overridable execute method**
  (`FivetranAccountComponent.execute()` and `PowerBIWorkspaceComponent.
  build_semantic_model_refresh_asset_definition()` do; `AzureDataFactoryComponent`
  inlines its trigger-and-poll logic in a private module function calling a
  private free function, `_get_adf_client`, with no override point).
  Fallback seam: monkeypatch the module-level free function for the
  process's lifetime when `demo_mode=True` — a scoped patch-and-restore
  doesn't work, since generated sensor/asset closures resolve it from
  module globals at *call* time, not definition time. Also: its
  `assets_by_pipeline_name` overrides a pipeline's spec `key:`, but its
  observation sensor re-derives `asset_key` from the raw object name
  instead of the override, so a key override there produces a dangling
  observation — verify by evaluating the sensor directly, not just
  `dg list defs`. See `component-feedback/2026-09-03-azure-data-factory-demo-mode-seam.md`.
  (verified 2026-09-03)
- **Azure SDK / msrest models coerce naive datetimes to UTC-aware on
  assignment** (`azure.mgmt.datafactory.models.RunFilterParameters` does
  this) — a demo-mode fixture comparing its own naive timestamps against
  those fields raises `TypeError: can't compare offset-naive and
  offset-aware datetimes`. Give any Azure-SDK-backed fixture UTC-aware
  timestamps from the start. (verified 2026-09-03)

## Don't rebuild platform features

- Dagster+ has **native alert policies** for Slack, Teams, email, and
  PagerDuty, covering run failures, asset check failures, freshness violations,
  and schedule/sensor failures. Never hand-roll alerting in a demo — it implies
  the platform lacks something it has. Show it in the UI instead. (2026-08-25)

- Jobs: use `define_asset_job` with `AssetSelection`. Never call asset functions
  inside a job definition. (2026-08-26)

- **Verify each feature-floor item actually appears in `dg list defs --json`.**
  A component declaring a config field does not mean it builds anything from it
  — a registry component with a `polling_sensor` field that never wired a sensor
  silently sank three consecutive builds. Confirm presence in the definitions
  listing; don't assume the component honoured its own config. (2026-08-26)
- Read the most recent successful project in `demos/` for established
  conventions (warehouse setup, check style, README shape) before inventing your
  own. Cheap, and it keeps builds consistent. (2026-08-26)

- **Never route an external system through a home-made component**, and check
  *every* system, not just the easy ones — partial compliance (3 of 4 systems
  right) reads as success and hides the gap. **The system named in the demo
  thesis is the one most likely to be missed and the one that matters most.**
  rvu-tempcover needed three builds: home-made component for everything, then
  Fivetran/Power BI/dbt fixed but the thesis's own named incumbent (ADF) left
  as prose — even after this rule was already written down once. Check the
  system-to-component-ID mapping against the brief's thesis sentence
  specifically, every build; writing the rule down once isn't sufficient.
  (2026-09-04, reconfirmed 2026-09-03)
- **A component name must identify a system or domain concept**, never a
  technique. `GraphFirstAsset` / `DemoAsset` / `StubComponent` / `MockAsset` are
  always wrong — Dagster already has assets. (2026-08-27)

## Dead ends

- **Never plant a failure in a demo.** No anomalies, corrupt partitions, or
  missing data — not behind a flag. A demo that can fail will fail live, on the
  path nobody rehearsed. Build the checks and explain what they'd catch in
  production, against a green graph. Corollary: nothing to heal, so no heal
  asset, heal job, or reset object; a disconnected `healed_partitions` node
  reads as scaffolding. Briefs cannot override this. (2026-08-25)
