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

- `pex` installed and `dagster-cloud` a project dependency, or `--build-method
  local` fails. `--package-name` is the module holding `Definitions` — verify
  with `python -c "import <pkg>"`.
- `dbt_project/` must live **inside** the package dir, and dbt's
  `target/manifest.json` / `.local_defs_state/` must be force-included in
  `[tool.hatch.build.targets.wheel]` — both gitignored by default, so they
  silently don't ship otherwise. Verify with `unzip -l dist/*.whl`.
- Run `dg utils refresh-defs-state` before deploying state-backed components
  (dbt, Fivetran), or the location fails to load remotely. Activate the
  project venv before any `dagster-cloud` command.
- Agent sync after PEX upload takes several minutes — normal, use a blocking
  wait. Exit code 0 does **not** mean the location loaded — confirm with
  `dg api`, look for `LOADED`. Prefer `--build-method local` (PEX);
  `serverless deploy` (Docker) is the fallback for source-only deps.

## Dagster+ Serverless runtime

- Storage is **ephemeral** — local DuckDB files don't persist between runs.
  Give interactive demos via `dg dev`; treat the Dagster+ deployment as proof
  the project loads, not a place to run multi-run sequences.

## CLI (dagster-dg-cli 1.13.19)

- **Use `dg`, never legacy `dagster`.** `dg launch --assets '*'` (not
  `--select`) cannot validate a partitioned project — every build ships
  `validate_e2e.py` for that (`scripts/validate_demo.sh` calls it).
- `Definitions.resolve_implicit_job_def_def_for_assets(asset_keys)` — the
  doubled `def_def` is real, not a typo.
- `dagster-component init` doesn't scaffold a project — run `create-dagster
  project` first, then `init --auto-install --force`. Right after, `dg check
  defs` can transiently fail with `ModuleNotFoundError` even though `python
  -c "import <pkg>"` works — re-run it; stale plugin manifest, not a real
  break.
- `dg list defs --json` top-level keys are `assets` / `asset_checks` / `jobs`
  / `schedules` / `sensors`; each asset's key is `"asset_key"` (snake_case).
- `dg.build_schedule_from_partitioned_job(...)` has no `timezone` kwarg — it
  inherits the job's `partitions_def` timezone, and accepts a
  `MultiPartitionsDefinition` job as long as one dimension is time-partitioned
  (cron derives from that dimension automatically).

## APIs and schemas (dagster-dbt 0.29.19)

- dbt asset `kinds` derive from the manifest's `adapter_type` (DuckDB badges
  everything `duckdb`). Subclass `DagsterDbtTranslator`/`DbtProjectComponent`,
  override `get_asset_spec(self, manifest, unique_id, project)`, and
  `spec.replace_attributes(kinds={"dbt", "<real-warehouse>"})`.
- dbt asset keys are `<custom-schema>/<model_name>`. A DuckDB schema you
  create yourself (`CREATE SCHEMA raw`) is queried as plain `raw.table`; only
  dbt-duckdb's own schemas get a `main_<schema>` prefix, e.g. `main_staging.foo`.
- Multiple `DbtProjectComponent` instances on the same `project_dir` trigger
  `DuplicateDefsStateKeyWarning` — override the `defs_state_config`
  **property** (not a method) and fold `self.op.name` into the key.

## Freshness & alerting APIs (dagster 1.13.19 / dagster-msteams 0.29.19)

- `dg.FreshnessPolicy.time_window(fail_window=timedelta, warn_window=timedelta
  | None)` (or `.cron(deadline_cron, lower_bound_delta, timezone)`) set on
  `AssetSpec.freshness_policy` is enough — Dagster computes PASS/WARN/FAIL
  itself, no separate check definition needed.
- Jinja in `defs.yaml` can't do `AutomationCondition.eager() & other`, build a
  `timedelta` inline, or construct a `MultiPartitionsDefinition` (its dict arg
  isn't in the `dg` scope's supported set — see its docs for the exact list)
  — compose any of these in a `template_vars.py` sibling module as an
  `@dg.template_var` function and reference it as `"{{ fn_name }}"` (needs
  `template_vars_module: .template_vars` on the defs.yaml).
- On a `MultiPartitionsDefinition` asset, `context.partition_key` is a
  `MultiPartitionKey`; read each dimension via
  `partition_key.keys_by_dimension["dim_name"]` — works the same inside a
  `@dg.multi_asset` and a `@dg.asset_check`.
- `dagster_msteams.MSTeamsResource.hook_url` has no default — subclass with a
  placeholder + override `get_client()`. `make_teams_on_run_failure_sensor`
  builds its `TeamsClient` at fire time and defaults `STOPPED`, so it's safe
  to define even in demo mode. Native `dg.run_failure_sensor` (no third-party
  resource) is a fine fallback when the brief names no alerting channel.
- `dagster_azure.adls2.ADLS2Resource`'s `adls2_client`/`blob_client` are
  `@cached_method` properties that eagerly authenticate on first access — a
  demo-mode subclass must never touch them. `dagster_aws.s3.S3Resource` is
  safer: `get_client()` is a plain method, only builds a boto3 client when
  called, so a demo-mode subclass just skips calling it.

## Component-authoring gotchas

- Two `@dg.multi_asset(specs=[spec])` from the same factory need an explicit
  unique `name=`, or op registration collides across instances.
- A bare module-level `AssetSpec` variable *and* a `@dg.multi_asset` built
  from it in the same file duplicates the asset ("defined multiple times")
  — the autoloader picks up both. Inline the spec inside the decorator.
- `JobDefinition.execute_in_process()` has no `asset_check_selection` kwarg;
  checks on selected assets just run automatically.
- `pd.DataFrame([])` (empty list of dicts) has zero columns and breaks a
  DuckDB insert into a table with an existing schema — build via
  `pd.DataFrame(rows, columns=[...])` so an empty batch keeps its schema.

## Registry gaps (searched, ruled out for partitioned demo-mode ingestion)

- No component supports a partitioned, demo-mode-fakeable "poll an inbound
  file/event" source, on Azure or AWS: `adls_monitor`/`s3_monitor`/
  `sqs_monitor` poll a real bucket/queue with no demo-mode affordance;
  `database_replication` (Sling) has no `partitions_def`; `rest_api_fetcher`/
  `odata_ingestion` need a live endpoint; `fivetran_assets` keys by the
  connector's own tables, not a custom partition. Custom `dg.Component`
  subclass (rung 4) is the working pattern instead.

## Environment

- `profiles.yml` needs a working default path with the env var as an
  optional override: `{{ env_var('X_PATH', 'demo_data/demo.duckdb') }}`.
- `GH_TOKEN` reads as literal `proxy-injected` when the GitHub proxy handles
  auth — treat as unset.
- Gmail exposes `create_draft` but no send; draft + mobile push, never report
  the missing send as a failure.
- Briefs and `state/ledger.json` must live on `main`; anything on an unmerged
  branch is invisible to Factory and the run silently no-ops.
