# LEARNINGS

Verified facts from previous Demo Factory runs. Read before building; update
after every run (step 10 of the Demo Factory prompt).

**Only verified facts.** Confirmed by running it or reading source. A wrong
entry is worse than none — the next run will trust it.

Keep under ~120 lines. When it grows past that, delete the least useful entries
rather than letting it sprawl. No prospect-specific content — tooling facts
here, prospect facts in the brief.

---

## Deployment packaging — READ BEFORE DEPLOYING

`./scripts/preflight_deploy.sh <slug> <pkg>` checks every item below in ~20s.
Run it before the first deploy; a 2026-08-24 run took seven attempts without it.

- `pex` installed and `dagster-cloud` in `pyproject.toml` dependencies (not
  just on PATH). (2026-08-24)
- **`deploy_demo.sh` must activate the project venv** (`source
  "$PROJECT_DIR/.venv/bin/activate"`). `dagster-cloud` is a project dep, so
  without it the deploy dies with a bare `dagster-cloud: command not found`
  *after* validation passes, which reads like a deploy failure. Fixed in the
  script 2026-08-24. (2026-08-24)
- Use `--module-name <pkg>.definitions`, not `--package-name <pkg>` — a
  `create-dagster` project has no `Definitions` at the top level. (2026-08-24)
- **`dbt_project/` must live INSIDE the Python package directory** or it does
  not ship in the wheel. (2026-08-24)
- **Gitignored files do not ship in the wheel.** For `.local_defs_state/` use
  `[tool.hatch.build.targets.wheel] artifacts = ["src/<pkg>/defs/.local_defs_state/**"]`.
  Do **not** also list the same path in `force-include` — they collide with "A
  second file is being added to the wheel archive at the same path". (2026-08-24)
- **Run `dg utils refresh-defs-state` before deploying** with state-backed
  components (dbt, Fivetran), then verify: `uv build --wheel` and
  `unzip -l dist/*.whl | grep -E "manifest|defs_state"`. (2026-08-24)
- `dagster-cloud deployment delete-location` takes the location as a
  **positional** arg, not `--location-name`. (2026-08-24)

## Deployment — waiting

- The agent sync after PEX upload routinely takes **several minutes**. Normal,
  not a hang. Run the deploy as a background task and wait on it; don't poll in
  a tight loop. (2026-08-24)
- A successful deploy command does **not** mean the location loaded. Confirm
  with `dg api code-location list --json` and read the `status` field —
  `deployment list-locations` prints names/images only, no status. (2026-08-24)
- `LOADED` means the definitions parsed. It does **not** mean assets
  materialize in the cloud. (2026-08-24)
- `deploy-python-executable ... --build-method local` (PEX) is the path. Docker
  *is* available, so plain `serverless deploy` is a genuine fallback if PEX
  fails on a source-only dependency. (2026-08-23)

## Commands and flags

- **Briefs and `state/ledger.json` must live on `main`.** Factory reads both
  from the default branch; anything on an unmerged `claude/` branch is invisible
  and the run becomes a silent no-op. (2026-08-24)
- **Use `dg`, never the legacy `dagster` CLI.** `dg launch` takes `--assets`;
  the old CLI took `--select`. Options: `--assets`, `--job`, `--partition`,
  `--partition-range <start>...<end>`, `--config`. (2026-08-24)
- **`dg launch --assets '*'` cannot validate a project with partitioned
  assets** — it exits immediately with "Asset has partitions, but no
  '--partition' option was provided". Since partitions are near-mandatory per
  the feature floor, every demo needs an in-process harness:
  `defs.resolve_implicit_job_def_def_for_assets(keys)` then
  `job.execute_in_process(instance=..., partition_key=..., asset_selection=...)`
  looped over partitions. Much faster than one CLI call each. Crib
  `demos/northwind-logistics/scripts/validate_end_to_end.py`. (2026-08-24)
- **`DailyPartitionsDefinition(end_date=X)` excludes X's own partition** —
  `end_date="2026-08-25"` makes `2026-08-23` the last key. Demo-window
  constants must match or looping fails with `DagsterUnknownPartitionError`.
  (2026-08-24)
- `dagster-component init` does **not** scaffold a project — run
  `create-dagster project` first. Always pass `--auto-install` to `init`/`add`
  or they prompt and hang unattended. (2026-08-23)
- `create-dagster` does not create `<pkg>/components/`, but the registry entry
  point points at it — `dg check defs` fails with `ModuleNotFoundError` on a
  fresh scaffold until you `mkdir` it with an `__init__.py`. (2026-08-24)
- `components/__init__.py` must re-export each component class or the Dagster UI
  Components tab won't list them even when `dg list components` does.
  (2026-08-24)

## Build sequencing that worked

- **Default the demo warehouse env var in the package `__init__.py`**, not in
  `definitions.py`. `dg utils refresh-defs-state` loads components without
  importing `definitions.py`, so a default set there is missing when dbt parse
  runs ("Env var required but not provided"). `__init__.py` is imported by
  every component module, so `os.environ.setdefault()` there lands before any
  dbt subprocess spawns — and being absolute it resolves the same from any cwd,
  including the copy dagster-dbt makes under `.local_defs_state/`. (2026-08-24)
- Run `dg check defs` after each layer (ingestion → SaaS → dbt), not once at the
  end. Failures localize instead of compounding. (2026-08-24)
- **Dagster+ Serverless storage is ephemeral.** Each run is a fresh container;
  local DuckDB files do not persist between runs. Any sequence spanning
  multiple runs — including fail → rematerialize → recover — works locally and
  silently breaks in Serverless. Give the interactive demo via `dg dev`; treat
  the Dagster+ deployment as proof the project loads. (2026-08-24)

## Environment

- `GH_TOKEN` reads as literal `proxy-injected` when the GitHub proxy handles
  auth. Not a usable token — treat as unset and skip standalone-repo steps.
  (2026-08-23)
- Gmail exposes `create_draft` but no send. Draft + mobile push; never report
  the missing send as a failure. (2026-08-23)
- Skills committed under `.claude/skills/<name>/<version>/skills/<name>/SKILL.md`
  are **not** invokable via the Skill tool (it wants
  `.claude/skills/<name>/SKILL.md`). Read them as plain files instead.
  (2026-08-24)

## Component schemas and APIs

- **dbt asset `kinds` come from the manifest's `adapter_type`**, so a
  DuckDB-backed demo badges every model `duckdb`. No `get_kinds` hook exists —
  subclass and override `get_asset_spec(self, manifest, unique_id, project)`,
  then `spec.replace_attributes(kinds={"dbt", "snowflake"})`. No `@dataclass`
  decorator needed on the subclass. (2026-08-24)
- Regular assets and `AssetSpec`s accept `kinds={"snowflake"}` directly. Max 3
  per asset. Badge the prospect's stack, not the execution engine. (2026-08-24)
- Multiple `DbtProjectComponent` instances over **one** dbt project (different
  `select:` + `partitions_def`) is the documented way to handle mixed partition
  schemes, and they correctly **share** one defs-state key (derived from the
  project dir). The `DuplicateDefsStateKeyWarning` on load is expected and
  harmless — do not force distinct keys, which ships N copies of the manifest.
  (2026-08-24)
- A multi-partitioned upstream feeding a single-dimension downstream needs an
  explicit `dg.AssetDep(key, partition_mapping=dg.MultiToSingleDimensionPartitionMapping(
  partition_dimension_name="date"))`; the default same-key mapping never
  resolves. (2026-08-24)
- Jinja in `defs.yaml` has no `&` operator, so composed AutomationConditions
  (`eager() & all_deps_blocking_checks_passed()`) must be built in a
  `template_vars_module` and referenced by name. (2026-08-24)
- `dg.Definitions` has no `get_all_asset_specs()` — it is
  `resolve_all_asset_specs()`. (2026-08-24)
- A **blocking** check failing makes `execute_in_process` return
  `success=False` and raise `DagsterAssetCheckFailedError`. That is the check
  working; a validation harness must expect it on the anomaly partition.
  (2026-08-24)
- `SnowflakeResource` requires only `user`; everything else is optional. Give
  every field an `env('X', 'default')` fallback so demo mode needs no env vars.
  `{{ env.X }}` yields `None` when unset, which fails a required field.
  (2026-08-24)

## Registry gaps

- No component covers generic REST / carrier-rate APIs. Searched "rest api",
  "http polling", "rate limit", "carrier", "freight". Wrote a custom
  component. Worth contributing back. (2026-08-24)

## Dead ends — don't retry these

- **Never model recovery as an action inside Dagster.** No heal asset, no heal
  job, no reset object. Assets are idempotent — rematerializing a partition
  re-reads the source and picks up what's there now. Model late data as
  *source arrival timing* inside the mock (a JSON file under `demo_data/` the
  mock reads and writes), so a plain rematerialize is the whole recovery story.
  A `demo_control`/`healed_partitions` asset floats disconnected in the lineage
  graph and immediately reads as scaffolding. (2026-08-24)
