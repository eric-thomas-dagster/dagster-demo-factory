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

Never put prospect-specific content here. Tooling facts here, prospect facts
in the brief.

---

## Deployment

- `pex` + `dagster-cloud` must be **project dependencies**, not just a CLI on
  PATH, or `deploy_demo.sh` fails after validation passes.
- `dbt_project/` must live inside the package dir; gitignored build outputs
  (dbt `manifest.json`, defs-state) need `[tool.hatch.build.targets.wheel]
  force-include`/`artifacts` or they silently miss the wheel.
- Run `dg utils refresh-defs-state` before deploying state-backed components
  (Fivetran, dbt), and verify wheel *contents*
  (`unzip -l dist/*.whl | grep -E "manifest|defs_state"`), not config.
- Run `deploy_demo.sh` in the **foreground** — it already polls to LOADED.
  Exit 0 ≠ loaded; confirm with `dg api code-location list --json`.
- Dagster+ Serverless storage is **ephemeral** (fresh container per run) —
  local DuckDB/state files don't survive between runs. Give interactive
  demos via `dg dev`; the deployed location is proof-it-loads, not a
  multi-run recovery stage.

## CLI

- **`dg`, never legacy `dagster`** (`dg dev`, `dg launch --assets '*'`).
- `dg launch --assets '*'` **can't validate a partitioned project** ("no
  '--partition' option") — ship `validate_e2e.py`; `validate_demo.sh` calls it.
- `Definitions.resolve_implicit_job_def_def_for_assets(...)` — doubled
  `def_def` is the real name, not a typo.
- `dagster-component init` needs `create-dagster project` first and
  `--auto-install` or it hangs unattended.
- `dagster-component search` takes **one** positional arg (AND-ed terms);
  always pass `--json`.
- `dg list components` missing a custom component → re-run
  `dagster-component init --force`.

## APIs and schemas

- dbt asset `kinds` derive from manifest `adapter_type` (DuckDB → `duckdb`
  badge) — override via a `DbtProjectComponent` subclass's `get_asset_spec`
  + `spec.replace_attributes(kinds={...})`; no other hook.
- A bare `dg.AssetSpec` in `Definitions(assets=[...])` is the external/
  observed-only pattern — history only via
  `instance.report_runless_asset_event(...)`.
- `dg.ResolvedAssetSpec` (YAML) takes `partitions_def`, `automation_condition`,
  `freshness_policy`, `owners`, `metadata` inline. (dagster==1.13.20)
- `defs` is a `LazyDefinitions` until **called** — `defs()` gives the real
  `Definitions` (`.resolve_implicit_job_def_def_for_assets()` /
  `.get_repository_def()`), required for `validate_e2e.py`.
- `dg list defs --json` omits `partitions_def`/`freshness_policy`/`metadata`
  — check those via `defs().get_repository_def().asset_graph.get(key)`.
- An unpartitioned asset's plain `deps=` on a partitioned, unselected
  upstream executes standalone fine with no `partition_key`, both directions.
  (verified 2026-08-28, 09-02, 09-03)
- `DailyPartitionsDefinition` rejects the **current day** as a key —
  `validate_e2e.py` dates must be `<= today - 1`.
- **`deps:` in component YAML needs the full path-prefixed key**
  (`"marts/fct_x"`, not `"fct_x"`) for a namespaced target — a mismatch
  doesn't error, it silently creates a disconnected `group: default` stub.
  Check `dg list defs --json` for stray `"group_name": "default"` entries.
  (verified 2026-09-03)
- `DbtProjectComponent`'s `translation:` block takes `owners:`/`metadata:`
  applied to every asset the instance builds; `post_processing.assets[].
  attributes.metadata` on one `target:` **merges additively** on top —
  uniform owner/tier/domain plus per-asset overrides, no per-model repeats.
  (verified 2026-09-03)
- A dbt **`source`** (not `seed`) node whose `meta.dagster.asset_key` matches
  an asset defined elsewhere is dependency-only and unifies with it (not a
  duplicate) — lets a graph-first ingestion `AssetSpec` stand in for the
  table a real dbt project reads, rows loaded by a non-Dagster bootstrap at
  defs-load time. (demos/kapitus, reused demos/rvu-tempcover 2026-09-03)

## Environment

- `profiles.yml` needs a **working default** with env var override
  (`env_var('X', 'demo_data/demo.duckdb')`) — no-fallback-required breaks
  zero-setup.
- `GH_TOKEN` reads `proxy-injected` when the GitHub proxy handles auth —
  treat as unset.
- Gmail: `create_draft` only, no send. Draft + mobile push; not a failure.
- Briefs/`state/ledger.json` must live on `main` or Factory can't see them.

## Registry behaviour and conventions

- **Never assert a gap without searching** — registry has thin wrappers over
  core Dagster (`cron_schedule`, `automation_condition_applicator`) too.
- Prefer **workspace-style** components with an explicit mapping table
  (`assets_by_task_key`) over one instance per object.
  Convention: `translation:` field, `get_asset_spec(props)` hook,
  `polling_sensor`/`generate_sensor` (**default False**), `StateBackedComponent`
  with enumeration in the state-write path (no HTTP at load time).
- `fivetran`/`powerbi`/`braze` (searched 2026-09-03): no workspace component
  works with zero credentials — all require a real account to enumerate at
  state-write time. `GraphFirstAssetsComponent` covers the gap.
- No component declares no-op `AssetSpec`s from YAML for graph-first demos
  (searched 2026-08-28) — `GraphFirstAssetsComponent` (`demos/detroit-dwsd`)
  is reusable byte-for-byte; copy the file, update the import path, cite
  `component-feedback/2026-08-28-graph-first-assets.md`, don't re-search.
  (reused demos/trafigura 09-02, demos/rvu-tempcover 09-03)
- Community `cron_schedule`'s partitioned mode rejects
  `cron_expression`/`execution_timezone` with `partition_type`/`hour_of_day`
  together — use native `dg.build_schedule_from_partitioned_job(hour_of_day=
  ..., minute_of_hour=...)` for a specific local hour.

## Don't rebuild platform features

- Dagster+ has **native alert policies** (Slack/Teams/email/PagerDuty) —
  never hand-roll alerting.
- Jobs: `define_asset_job` + `AssetSelection`, never call asset functions
  inside a job body.
- Verify feature-floor items actually appear in `dg list defs --json` /
  the resolved `Definitions` — a config field doesn't mean it built anything.
