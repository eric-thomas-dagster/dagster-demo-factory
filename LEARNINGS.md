# LEARNINGS

Verified facts from previous Demo Factory runs. Read after `CLAUDE.md`, before
building. Every run updates it (step 10 of the Demo Factory prompt).

**Facts, not rules.** How a build *should* behave belongs in `CLAUDE.md`. This
file is for things that are true about the tooling — command forms, schemas,
version quirks, dead ends. If an entry reads like a policy, move it.

**Maintenance contract — every run does all three, in this order:**

1. **Invalidate.** If an entry is now wrong, fix or delete it. A stale entry is
   worse than none, because the next run trusts it.
2. **Prune.** Delete entries that no longer earn their context cost, or that
   are superseded by a better entry.
3. **Append.** Only then add what this run verified — by running it or reading
   source. Never speculation.

One or two lines each. No narrative, no run stories. Version-sensitive facts
carry the version they were checked against, so a future run knows when to
re-verify. **Hard cap ~100 lines — if over, cut the weakest before appending.**

**No tool-specific content and no registry inventories.** The registry has
~975 components; listing which exist would go stale immediately and is what
`dagster-component search --json` is for. Record *conventions and behaviours*
a source read reveals — a field's default, a shared class shape, a documented
override hook.

Never put prospect-specific content here. Tooling facts here, prospect facts
in the brief.

---

## Deployment

- `pex` must be installed and `dagster-cloud` a **project dependency** (not
  just a CLI on PATH), or `deploy_demo.sh` fails after validation passes.
- `--package-name` must be the module holding `Definitions`; verify with
  `python -c "import <pkg>"`.
- `dbt_project/` must live inside the package dir or it won't ship in the
  wheel. `.gitignore`d files (dbt `manifest.json`, defs-state) don't ship
  either — force-include via `[tool.hatch.build.targets.wheel]`.
- Run `dg utils refresh-defs-state` before deploying state-backed components
  (Fivetran, dbt) or the location fails to load remotely.
- Verify wheel *contents*, don't trust config:
  `unzip -l dist/*.whl | grep -E "manifest|defs_state"`.
- Run `deploy_demo.sh` in the **foreground**, in one call — it already polls
  to a terminal state; backgrounding it just re-implements that wait. Agent
  sync after PEX upload routinely takes several minutes; normal, not a hang.
- Deploy exit 0 does **not** mean the location loaded — confirm with
  `dg api code-location list --json` (`status: LOADED`). `LOADED` means
  definitions parsed, not that assets materialize.
- Dagster+ Serverless storage is **ephemeral** (fresh container per run) —
  anything spanning multiple runs (local DuckDB, state files) works locally
  and silently breaks in Serverless. Give interactive demos via `dg dev`;
  treat the deployed location as proof it loads, not as the recovery-sequence
  stage.

## CLI

- **Use `dg`, never the legacy `dagster` CLI** (`dg dev`, `dg launch
  --assets '*'` — note `--assets` not `--select`). (dagster-dg-cli 1.13.19)
- `dg launch --assets '*'` **cannot validate a partitioned project** — exits
  with "no '--partition' option was provided". Every build with partitions
  ships `validate_e2e.py`; `scripts/validate_demo.sh` calls it.
- `Definitions.resolve_implicit_job_def_def_for_assets(asset_keys)` is the
  real method name (doubled `def_def` is not a typo).
- `dagster-component init` does **not** scaffold a project (`create-dagster
  project` first) and needs `--auto-install` or it hangs unattended.
- `dagster-component search` takes **one** positional arg — put multiple
  terms inside one quoted string (AND-ed across id/name/description/tags).
  Always pass `--json`.
- If `dg list components` misses a custom component, re-run
  `dagster-component init --force`.

## APIs and schemas

- dbt asset `kinds` derive from the manifest's `adapter_type` (DuckDB badges
  everything `duckdb`) — override via `DagsterDbtTranslator.get_asset_spec`
  + `spec.replace_attributes(kinds={...})`. No other hook exists.
- `components/__init__.py` must re-export each component class or the UI
  Components tab won't list it even when `dg list components` does.
- A bare `dg.AssetSpec` in `Definitions(assets=[...])` (not wrapped in
  `@asset`) is the real external/observed-only asset pattern — Dagster wraps
  it into a zero-compute `AssetsDefinition`; its history is only
  `AssetObservation`/`AssetCheckEvaluation` via
  `instance.report_runless_asset_event(event)`.
- A checks-only job over such a non-executable asset **cannot** get a
  `PartitionsDefinition` even passed explicitly (`_infer_and_validate_
  common_partitions_def` only looks at `executable_asset_keys`, empty here)
  — evaluate the check directly and report it via
  `report_runless_asset_event` instead. (dagster==1.13.19)
- `dg.ResolvedAssetSpec` (the YAML-facing type) accepts `partitions_def`,
  `automation_condition`, `freshness_policy`, `owners` inline per entry, and
  `@dg.multi_asset(specs=[spec])` infers `partitions_def` from the spec
  itself — no redundant top-level `partitions_def=` kwarg needed.
  (dagster==1.13.20, verified 2026-08-28)
- The `@definitions`-decorated `defs` object in `<pkg>/definitions.py` is a
  `LazyDefinitions` (fields: `count`, `has_context_arg`, `load_fn`) until
  **called** — `defs()` resolves it to a real `Definitions` with
  `.resolve_implicit_job_def_def_for_assets()` / `.get_repository_def()`.
  Importing `defs` alone isn't enough for a script like `validate_e2e.py`.
  (verified 2026-08-28)
- `dg list defs --json` per-asset fields are a fixed subset (`asset_key`,
  `description`, `group_name`, `kinds`, `dependency_keys`, `owners`,
  `automation_condition`, `is_executable`, `source`) — no `partitions_def`,
  `freshness_policy`, or `metadata`. Verify those via
  `defs().get_repository_def().asset_graph.get(key)` directly, not the JSON
  listing. (verified 2026-08-28)
- An unpartitioned asset with a plain (non-argument) `deps=` edge to a
  partitioned, *unselected* upstream asset executes standalone fine via
  `job.execute_in_process(asset_selection=[...])` with no `partition_key` —
  ordering-only deps don't require the upstream partition to already exist.
  (verified 2026-08-28)

## Environment

- `profiles.yml`-style config needs a **working default** with the env var as
  an optional override (`{{ env_var('X_PATH', 'demo_data/demo.duckdb') }}`) —
  required-with-no-fallback ships a demo that won't start.
- `GH_TOKEN` reads as literal `proxy-injected` when the GitHub proxy handles
  auth — not a usable token, treat as unset.
- Gmail exposes `create_draft` but no send; routines run without approval
  prompts. Draft + mobile push; never report the missing send as a failure.
- Cloud env vars are **not** visible to the setup script — session shell only.
- Briefs and `state/ledger.json` must live on `main`; anything on an unmerged
  branch is invisible to Factory and the run silently no-ops.

## Registry behaviour and conventions

- **Never assert a registry gap without searching** — the registry includes
  thin wrappers over core Dagster calls (`cron_schedule`,
  `automation_condition_applicator`), so "this is core Dagster" is not
  evidence of absence. Search, always, with `--json`.
- Use the **workspace-style** component with an explicit mapping table in
  `defs.yaml` (e.g. `assets_by_task_key`), not one instance per external
  object. Reference: `databricks-workspace-bundles-demo`.
- **Workspace components share one convention**: `@public` class,
  `translation:` field, `@public get_asset_spec(props)` override hook,
  `polling_sensor` (alias `generate_sensor`, **default False** — set true or
  the demo never sees externally-triggered runs), `defs_state` +
  `defs_state_config`, `StateBackedComponent` inheritance with enumeration in
  the **state-write path** (no HTTP at load time — not a reason to reject
  one).
- No registry/native component declares a list of no-op `AssetSpec`s from
  YAML for graph-first (`pass`-bodied) demos — write one small custom
  component (`assets: list[dg.ResolvedAssetSpec]`, one
  `@dg.multi_asset(specs=[spec])` per entry); legitimate rung-4 case, since
  there's no integration domain to subclass. (searched 2026-08-28,
  dagster-community-components-cli 0.8.15)
- Community `cron_schedule` component's partitioned-job mode rejects
  `cron_expression`/`execution_timezone` combined with
  `partition_type`/`hour_of_day` in either direction (`CheckError`) — use
  native `dg.build_schedule_from_partitioned_job(hour_of_day=...,
  minute_of_hour=...)` directly when a specific local hour matters.
  (verified 2026-08-28)

## Don't rebuild platform features

- Dagster+ has **native alert policies** (Slack/Teams/email/PagerDuty) for
  run failures, check failures, freshness violations, schedule/sensor
  failures. Never hand-roll alerting in a demo.
- Jobs: use `define_asset_job` with `AssetSelection`. Never call asset
  functions inside a job definition.
- **Verify each feature-floor item actually appears in `dg list defs
  --json`** (or, for fields the JSON omits, in the resolved `Definitions`
  object directly) — a component declaring a config field doesn't mean it
  built anything from it.
