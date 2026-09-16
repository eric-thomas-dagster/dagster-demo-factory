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

`./scripts/preflight_deploy.sh <slug> <package_name>` checks all of these in
~20s. Use it instead of discovering them one deploy cycle at a time.

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

- `dg launch` options: `--assets`, `--job`, `--partition`,
  `--partition-range <start>...<end>`, `--config` / `--config-json`.
- **`dg launch --assets '*'` cannot validate a partitioned project** — it exits
  with "Asset has partitions, but no '--partition' option was provided". Every
  build must ship `validate_e2e.py`; `scripts/validate_demo.sh` calls it.
- `Definitions.resolve_implicit_job_def_def_for_assets(asset_keys)` is the real
  method name — the doubled `def_def` is not a typo.
- `dagster-component init` does **not** scaffold a project — it writes AI-tool
  config and wires the `registry_modules` entry point into an existing one. Run
  `create-dagster project` first.
- Pass `--auto-install` to `dagster-component init` / `add`, or they prompt and
  hang forever unattended.
- `dagster-component search` takes **one positional argument**. Multiple
  positional terms are rejected. Put several terms *inside* one quoted string —
  they're AND-ed across id, name, description, tags, keywords, agent_hints.
  `--json` returns `{id, score, matched_fields, matched_terms, category,
  produces, description, validation_level}`.
- If `dg list components` misses custom components, re-run
  `dagster-component init --force`.
- **`dg check defs` / `dg list defs` failing with `ModuleNotFoundError: No
  module named '<pkg>'` has two causes.** (1) Transient, right after
  `uv sync` — retry once. (2) Non-transient PATH order: prepending another
  bin dir (e.g. for `uvx`) *after* `source .venv/bin/activate` makes `dg`
  resolve to a global binary with no visibility into the project's editable
  install. Fix: activate the venv **last**, never touch PATH after.
  (verified 2026-09-15, demos/bokadirekt)

## APIs and schemas

- dbt asset `kinds` derive from the manifest's `adapter_type`. No `get_kinds`
  hook — subclass `DagsterDbtTranslator` (or override `DbtProjectComponent.
  get_asset_spec`) and `spec.replace_attributes(kinds={"dbt", "<warehouse>"})`.
- `components/__init__.py` must re-export each component class, or the UI
  Components tab won't list them even when `dg list components` does.
- **`AssetSpec.with_io_manager_key(key)` sets a system metadata entry that
  determines the built op's output Dagster type** (`Any` if present, else
  `Nothing`). Chaining `.replace_attributes(metadata=...)` off the spec
  *before* that call instead of after silently drops the key and reverts the
  type to `Nothing` — a real DataFrame return then raises
  `DagsterTypeCheckError` at runtime, not at `dg check defs`. Always chain
  metadata merges off the *latest* spec variable.
- **`dg.AssetSelection.assets(...)` rejects a `SourceAsset`** (what
  `@dg.observable_source_asset` returns) for a sensor/schedule job — use
  `dg.AssetSelection.keys(dg.AssetKey(...))` instead. The decorator does
  accept `kinds=` though, so badging still works normally.
  (verified 2026-09-15, demos/bokadirekt)
- **A component's `ResolvedAssetSpec.key` YAML field is a plain string only**
  (slash-joined for multi-segment keys, e.g. `"bu/raw_table"`) — unlike
  `deps:`, which accepts a list of strings. Passing a YAML list for `key:`
  fails schema validation with "not valid under any of the given schemas"
  and no clearer message. (verified 2026-09-12, demos/umicore)

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
  branch is invisible to Factory and the run silently no-ops. If a `git fetch
  origin <ref1> <ref2>` fails because one ref doesn't resolve, the **whole**
  fetch aborts silently on the other ref too — fetch (or check) each ref
  separately before concluding a branch is missing upstream content.
- **Prefer `uv add <pkg>` over `pip install <pkg>`** for a dependency whose
  transitive deps include a package with a legacy setup.py-only sdist (e.g.
  `dagster-databricks` → `pyspark`) — plain `pip install` fails building the
  wheel (`AttributeError: install_layout`, a setuptools/legacy-setup.py
  incompatibility in this environment); `uv add` builds/resolves the same
  package successfully. (verified 2026-09-12, demos/umicore)

## Registry behaviour and conventions

- **Never assert a registry gap without searching, with `--json`, every time.**
  The registry includes thin wrappers over core Dagster calls, so "this is core
  Dagster" is not evidence of absence.
- **Workspace-style components share one convention**: `@public` class,
  `translation:` field, `@public get_asset_spec(props)` override hook,
  `polling_sensor` (alias `generate_sensor`), `defs_state` +
  `defs_state_config`, `StateBackedComponent` inheritance — enumeration happens
  in the state-write path, so "it queries a live connection at load time" is
  not a valid objection. Plain resource/IO-manager components (e.g.
  `mssql_io_manager`) do **not** follow this convention — it's specific to
  components that trigger/observe external jobs, not to persistence.
- **A component's default for its observation/polling-sensor field varies —
  read the field, don't assume the convention default (off) holds
  everywhere.** Some native workspace-style components ship with **no such
  field at all**, unlike siblings that do — check before assuming the
  convention is complete; adding the sensor in a subclass is rung 3, not
  rung 4. (verified 2026-09-12, demos/umicore)
- Jobs: use `define_asset_job` with `AssetSelection`. Never call asset functions
  inside a job definition.
- **Verify each feature-floor item actually appears in `dg list defs --json`.**
  A component declaring a config field does not mean it builds anything from
  it — confirm presence in the definitions listing, don't assume the component
  honoured its own config. Note: this CLI's `--json` output does not surface
  `partitions_def` or `freshness_policy` at all (any component) — verify those
  two specifically by loading `Definitions` in Python and checking
  `assets_def.partitions_def` / `asset_spec.freshness_policy` instead.

## dbt

- **A `DbtProjectComponent` instance covering a whole project is one multi-asset
  node.** Materializing a proper subset of its models (e.g. one layer only, via
  `asset_selection=[...]` with no explicit check keys) can still crash: a
  cross-model generic test (e.g. `equal_rowcount` with `other_model: ref(...)`
  declared on a *downstream* model, referencing an *upstream* one) gets eagerly
  pulled into dbt's indirect test selection because the upstream model is
  selected, even though the downstream model the test is declared on isn't
  selected/built yet (`Catalog Error: Table ... does not exist`). Reproduces via
  `dbt-core` 1.11 / `dagster-dbt` 0.29.21 selecting only the upstream layer.
  Relevant to any demo whose narrative materializes dbt layers incrementally
  (build-by-hand style) rather than the whole project at once — verify
  layer-by-layer materialization explicitly, not just a full-project run, when
  a generic test's `other_model` crosses layers. (verified 2026-09-16,
  demos/neighborhood-intelligence-taxi)

## Partitions

- **`dg.MultiPartitionKey({"dim1": "...", "dim2": "..."})` works directly as
  the `partition_key` argument to `job.execute_in_process(...)`** — no extra
  conversion needed. Its string form renders as `dim1_value|dim2_value` (order
  follows the dict passed to `MultiPartitionsDefinition`).
- A downstream asset can depend via a plain `deps=` edge on an upstream asset
  with a **completely different (or absent) `partitions_def`**, with zero
  `PartitionMapping` config, as long as no asset body actually reads the
  upstream's partition-scoped data — confirmed across mixed multi-partitioned /
  daily-partitioned / unpartitioned chains in the same graph (demos/eon-sverige,
  demos/detroit-dwsd).
- **dbt models can be a small full-refresh transform over the whole
  accumulated raw window, with Dagster's partition bookkeeping on the mart
  independent of dbt's own execution grain** — i.e. the SQL doesn't need to
  filter to `{{ partition_key }}` for the Dagster partition to still be
  tracked correctly. Reused across demos/rvu-tempcover, demos/kapitus,
  demos/partners-fcu.
