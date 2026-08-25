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

`./scripts/preflight_deploy.sh <slug>` checks all of these in ~20s.

- `pex` installed and `dagster-cloud` a project dependency (not just a CLI on
  PATH), or `--build-method local` fails.
- `--package-name` is the module holding `Definitions`, not the project dir
  name — verify with `python -c "import <pkg>"`.
- `dbt_project/` must live **inside** the Python package dir, and dbt
  `target/manifest.json` / defs-state must be force-included in
  `[tool.hatch.build.targets.wheel]` — both are normally gitignored, and
  gitignored files don't ship. Verify with `unzip -l dist/*.whl`, don't trust
  config.
- Run `dg utils refresh-defs-state` before deploying state-backed components
  (dbt, Fivetran), or the location fails to load remotely.
- Activate the project venv before `dagster-cloud` deploy commands, or it's
  "command not found" after validation already passed.
- Agent sync after PEX upload routinely takes several minutes — normal, not a
  hang; use a blocking wait, don't poll tightly. Exit code 0 does **not** mean
  the location loaded — confirm with `dg api`, look for `LOADED` (which itself
  only means definitions parsed, not that assets materialize in the cloud).
- Prefer `deploy-python-executable --build-method local` (PEX); `serverless
  deploy` (Docker) is a real fallback for source-only deps.

## Dagster+ Serverless runtime

- Storage is **ephemeral** — local DuckDB files don't persist between runs.
  Give interactive demos via `dg dev`; treat the Dagster+ deployment as proof
  the project loads, not a place to run multi-run sequences.

## CLI (dagster-dg-cli 1.13.19)

- **Use `dg`, never legacy `dagster`.** `dg dev`, `dg launch --assets '*'`
  (not `--select`), `dg check defs`/`yaml`, `dg list defs`/`components`.
- `dg launch --assets '*'` **cannot validate a partitioned project** — exits
  with "Asset has partitions, but no '--partition' option was provided".
  Every build ships `validate_e2e.py` (`scripts/validate_demo.sh` calls it).
- `Definitions.resolve_implicit_job_def_def_for_assets(asset_keys)` — the
  doubled `def_def` is the real method name, not a typo.
- `dagster-component init` does **not** scaffold a project — run
  `create-dagster project` first, then `init --auto-install --force` to wire
  the registry. Missing `--auto-install` on `init`/`add` hangs unattended.
- If `dg list components` misses custom components, re-run
  `dagster-component init --force`.
- `dg list defs --json` emits `"asset_key"` (snake_case), not `"assetKey"` —
  fixed in `scripts/validate_demo.sh`, which was silently reporting "0 assets
  discovered" on every run.

## APIs and schemas (dagster-dbt 0.29.19)

- dbt asset `kinds` derive from the manifest's `adapter_type` (DuckDB badges
  everything `duckdb`). No `get_kinds` hook — subclass `DagsterDbtTranslator`
  (or `DbtProjectComponent`), override
  `get_asset_spec(self, manifest, unique_id, project)`, and
  `spec.replace_attributes(kinds={"dbt", "<real-warehouse>"})`.
- dbt asset keys are `<custom-schema>/<model_name>` (e.g. `staging/foo`), not
  a flat model-name key.
- `components/__init__.py` must re-export each component class, or the UI
  Components tab won't list them even when `dg list components` does.
- Multiple `DbtProjectComponent` instances on the same `project_dir`
  (different `select`s) trigger `DuplicateDefsStateKeyWarning` — the base
  class keys defs state by project dir alone. Override the `defs_state_config`
  **property** (not a method — the warning text names the wrong symbol) and
  fold `self.op.name` into the key.

## Component-authoring gotchas

- Two `@dg.multi_asset(specs=[spec])` functions with the **same Python
  function name**, built from two instances of one factory component, collide
  in op registration — a confusing "op X does not have input Y" error mixing
  up deps between instances. Pass an explicit unique `name=` to `multi_asset`.
- `oracle_resource` / `postgres_resource` (registry `resource` category) fit
  the "resource seam" demo-mode pattern well — plain `get_connection()`
  returning a raw driver connection, all-required fields. Subclass, add
  demo-safe defaults on every field, override `get_connection()` as a
  `@contextmanager` yielding a duckdb connection in demo mode.
- `JobDefinition.execute_in_process()` has no `asset_check_selection` kwarg;
  checks on selected assets just run automatically. To prove a check catches
  bad data, mutate its real data source and rematerialize the checked asset.
- DuckDB: a schema you create yourself (`CREATE SCHEMA raw`) is queried as
  plain `raw.table`. Only dbt-duckdb's own schemas get a `main_<schema>`
  prefix (its `generate_schema_name` macro), e.g. `main_staging.foo`. Don't
  assume `main_` outside dbt-created schemas.
- `pd.DataFrame([])` (empty list of dicts) has zero columns and breaks a
  DuckDB insert into a table with an existing schema. A generator that can
  return zero rows must build via `pd.DataFrame(rows, columns=[...])`.

## Registry gaps (searched, ruled out for partitioned demo-mode ingestion)

- `database_replication` (Sling-backed): no `partitions_def` support; its I/O
  seam is `SlingResource.replicate()` against a real connection string, not
  fakeable without a live source+target DB.
- `rest_api_fetcher` / `odata_ingestion`: need a live endpoint at materialize
  time, no demo-mode seam.

## Project config

- `profiles.yml` needs a **working default path** with the env var as an
  optional override: `{{ env_var('X_DUCKDB_PATH', 'demo_data/demo.duckdb') }}`.

## Environment

- `GH_TOKEN` reads as literal `proxy-injected` when the GitHub proxy handles
  auth. Not a usable token — treat as unset.
- Gmail exposes `create_draft` but no send, and routines run without approval
  prompts. Draft + mobile push; never report the missing send as a failure.
- Cloud env vars are **not** visible to the setup script — session shell only.
- Briefs and `state/ledger.json` must live on `main`; anything on an unmerged
  branch is invisible to Factory and the run silently no-ops.
