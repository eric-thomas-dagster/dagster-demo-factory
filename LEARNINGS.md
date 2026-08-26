# LEARNINGS

Verified facts from previous Demo Factory runs. Read after `CLAUDE.md`, before
building. Every run updates it (step 10 of the Demo Factory prompt).

**Facts, not rules.** How a build *should* behave belongs in `CLAUDE.md`. This
file is for things that are true about the tooling. If an entry reads like a
policy, move it.

**Maintenance contract, every run, in order:** 1) **Invalidate** wrong entries
(fix/delete). 2) **Prune** entries that no longer earn their context cost.
3) **Append** only what this run verified by running it or reading source.

One or two lines each. No narrative, no run stories. Carry the version for
version-sensitive facts. Hard cap ~100 lines.

---

## Python gotcha that wastes a whole debug cycle

- **Never use `from __future__ import annotations`** anywhere Dagster reads a
  `context` annotation (`@asset`, `@asset_check`, `@op`). Dagster's
  `_validate_context_type_hint` compares the annotation by *identity*; the
  future-annotations string form fails with `Cannot annotate context
  parameter with type dg.AssetExecutionContext` — reads like an import bug,
  isn't. (dagster 1.13.19, verified 2026-08-26)

## Deployment packaging — check before deploying

`./scripts/preflight_deploy.sh <slug>` checks all of these in ~20s.

- `pex` must be installed, or `--build-method local` fails immediately.
- `dagster-cloud` must be a project dependency, not just a CLI on PATH.
- `dbt_project/` must live **inside** the Python package dir or it won't ship
  in the wheel.
- `.gitignore`d files don't ship (dbt `target/manifest.json`, defs-state) —
  force-include via `[tool.hatch.build.targets.wheel] artifacts =
  ["src/<pkg>/defs/.local_defs_state/**", ...]`.
- Run `dg utils refresh-defs-state` before deploying when using state-backed
  components (Fivetran, dbt, or any `StateBackedComponent` subclass, e.g.
  `fabric_workspace`), or the location fails to load remotely.
- `scripts/deploy_demo.sh` invokes `--module-name "$PKG.definitions"`, not
  `--package-name` — both flags exist on `deploy-python-executable`, but
  `--module-name` pointing at `definitions.py` is what the working script
  uses. (verified 2026-08-26)
- `deploy_demo.sh` must activate the project venv first, or deploy dies with
  `dagster-cloud: command not found` after validation passed.

## Deployment — timing and confirmation

- Agent sync after PEX upload routinely takes several minutes. Normal, not a
  hang. Blocking wait on the background task; don't poll tightly.
- Exit code 0 does **not** mean the location loaded — confirm with `dg api`,
  look for `LOADED`. `LOADED` means definitions parsed, not that assets
  materialize.
- Storage is **ephemeral** in Serverless — local DuckDB files don't persist
  between runs. Give interactive demos via `dg dev`; Dagster+ is proof the
  project loads, not the multi-run recovery-sequence stage.

## CLI

- **`dg`, never legacy `dagster`.** `dg launch --assets '*'` exits on any
  partitioned asset ("no '--partition' option") — every build ships
  `validate_e2e.py`; `scripts/validate_demo.sh` calls it.
- `Definitions.resolve_implicit_job_def_def_for_assets(asset_keys)` — the
  doubled `def_def` is not a typo.
- `dagster-component init`/`add` must run **from inside the project
  directory** — run from the parent `demos/` dir and it silently writes the
  AI-tool config files there instead, no error. Also needs `--auto-install`
  or it hangs waiting for a prompt. Run `create-dagster project` first; `init`
  only wires an existing project, it doesn't scaffold one.
- `dagster-component search` takes **one** positional argument — put several
  terms inside one quoted string, they're AND-ed. Use `--json`.
- If `dg list components` misses a custom component, re-run
  `dagster-component init --force`, and check `components/__init__.py`
  re-exports the class (the UI Components tab needs the re-export even when
  `dg list components` already shows it).

## APIs and schemas

- dbt asset `kinds` derive from the manifest's `adapter_type` — subclass
  `DagsterDbtTranslator`, override `get_asset_spec(...)`, and
  `spec.replace_attributes(kinds={"dbt", "snowflake"})`.
- Assets/`AssetSpec`s accept `kinds={"snowflake"}` directly. Max 3 per asset.
- **`post_processing.assets[].target/attributes` in `defs.yaml` is generic to
  every component**, not just dbt (`Definitions.map_asset_specs` under the
  hood, `target` via `AssetSelection.from_string`). Verified on a subclassed
  `StateBackedComponent` (`fabric_workspace`) to attach `freshness_policy`/
  `automation_condition` from outside. `template_vars_module: .template_vars`
  + `@dg.template_var` functions supply the `{{ name }}` values. (2026-08-26)
- `fabric_workspace`'s real attribute shape is a **nested** `workspace:`
  block (`{workspace_id, tenant_id, client_id, client_secret}`), matching the
  class docstring's example — the auto-generated `schema.json`/`example.yaml`
  from `dagster-component add` show flat top-level fields that don't match
  and will fail YAML resolution. Trust the Python docstring over the
  generated example when they conflict. (verified 2026-08-26)
- `cron_schedule`'s partitioned-job path builds its own `partitions_def`
  internally and passes it to `define_asset_job` — it must be **equal**
  (same `start_date`, same default-UTC timezone) to the assets' own
  `partitions_def`, or the job build fails. (verified 2026-08-26)

## Registry components that already exist (search before writing)

- Scheduling: `cron_schedule` (plain cron or partitioned-job cron via
  `partition_type`/`partition_start` — daily/weekly/monthly/hourly, or
  `multi` = one date axis + one static axis), `interval_schedule`,
  `automation_condition_applicator`, `per_partition_backfill_job`.
- Microsoft Fabric: `fabric_workspace` (`StateBackedComponent` — discovery
  happens once in `write_state_to_path`/`_list_items`, not every load),
  `fabric_pipeline_trigger_job`, `fabric_lakehouse_resource`,
  `fabric_lakehouse_io_manager`, `dataframe_to_fabric_lakehouse`,
  `fabric_capacity_admin_job`. Plus ~66 Azure, ~18 Databricks.
- `enhanced_data_quality_checks` exists but is a generic DataFrame
  column-stats library (null/range/correlation/Benford) — doesn't fit a
  partition-scoped SQL gate on a specific cross-table business rule. Native
  `@dg.asset_check` is correct there; it's core Dagster, not a registry rung.
- **Subclassing a workspace-style `StateBackedComponent` for demo mode + an
  explicit mapping table works** (rung 3): override the one discovery call
  (`_list_items`) for demo mode; override `build_defs_from_state` fully (not
  just the parent's per-type `_build_*_asset` helpers) when the mapping table
  needs to drive per-item `partitions_def`/deps the parent's import-flag
  split doesn't support. Verified end-to-end: `fabric_workspace` -> 17
  assets, one instance, one `assets_by_item_name` table. (2026-08-26)

## Don't rebuild platform features

- Dagster+ has **native alert policies** (Slack/Teams/email/PagerDuty) for
  run failures, check failures, freshness violations, schedule/sensor
  failures. Never hand-roll alerting in a demo. Show it in the UI instead.

## Dead ends

- **Never plant a failure in a demo.** No anomalies, corrupt partitions,
  missing data — not behind a flag. Build the checks, explain what they'd
  catch in production, against a green graph. No heal asset/job/reset object.
- **Cross-table rate metrics need matching populations.** A demo-credibility
  bug, not a check failure: `delinquent_count / total_contracts` where the
  numerator is drawn from a broader population than the denominator (all
  serviced payments vs. only that day's new originations) can silently
  produce a >100% rate. Pick both from the same query population, and
  eyeball `validate_e2e.py`'s printed row counts before calling a demo done.
  (2026-08-26)

## Environment

- `GH_TOKEN` reads as literal `proxy-injected` when the GitHub proxy handles
  auth — not a usable token, treat as unset.
- Gmail exposes `create_draft` but no send. Draft + mobile push; never report
  the missing send as a failure.
- Briefs and `state/ledger.json` must live on `main`; anything on an unmerged
  branch is invisible to Factory.
- A branch-name collision on push (`claude/demo-<slug>-<date>` already exists
  on the remote from an abandoned/reset prior attempt) isn't a merge — rename
  the local branch with a `-r2` suffix and push that instead of forcing.
  (2026-08-26)
