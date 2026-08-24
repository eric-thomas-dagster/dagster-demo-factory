# LEARNINGS

Verified facts from previous Demo Factory runs. Read before building; update
after every run (step 10 of the Demo Factory prompt).

**Only verified facts.** Confirmed by running it or reading source. A wrong
entry is worse than none — the next run will trust it.

Keep under ~120 lines. When it grows past that, delete the least useful entries
rather than letting it sprawl. No prospect-specific content — tooling facts
here, prospect facts in the brief.

---

## Platform constraint — READ BEFORE DESIGNING LOCAL-STORAGE DEMOS

**Dagster+ Serverless gives each run its own ephemeral disk** (per Dagster's
own docs — no documented persistent local-storage mount). A local DuckDB
file written by one run is NOT guaranteed to exist for a later, separate
run. Any demo whose story spans multiple runs (heal → rematerialize a
partition → automation-triggered downstream runs) with a local-file
warehouse must re-seed upstream data deterministically at the start of every
run that needs it. Small cross-run *control* state (a "healed partitions"
flag) can live in Dagster's own event log instead — an asset's
materialization metadata, read via
`instance.get_latest_materialization_event()` — since that's Dagster+'s own
managed backend and does survive across runs. (2026-08-24)

## Deployment packaging — READ BEFORE DEPLOYING

Run `./scripts/preflight_deploy.sh <slug>` before the first deploy.

- `pex` must be installed; `dagster-cloud` must be a project dependency, not
  just a CLI tool on PATH; `--package-name` must be the module holding
  `Definitions`; `dbt_project/` must live INSIDE the package dir. (2026-08-24)
- **hatchling's wheel only respects the ROOT `.gitignore`.** A nested
  `.local_defs_state/.gitignore: *` does NOT stop hatchling including most
  of that dir by default — except `target/` (root `.gitignore` has a
  generic `target/` rule, and dbt's compiled `manifest.json` lives there).
  Force-include exactly `.../project/target/manifest.json`; force-including
  the whole `.local_defs_state` tree double-adds files hatchling already
  included and errors "second file being added at same path." (2026-08-24)
- Run `dg utils refresh-defs-state` before deploying state-backed components
  (Fivetran, dbt) or the location fails to load remotely. Verify wheel
  contents rather than trusting config: `uv build --wheel` then
  `unzip -l dist/*.whl | grep manifest.json`. (2026-08-24)
- `dg launch --assets '*'` unconditionally fails the instant ANY selected
  asset is partitioned ("no '--partition' option was provided") — no
  "latest partition of everything" shortcut exists. `validate_demo.sh` step
  5 now warns instead of failing on this specific error; validate partitioned
  assets separately (loop `dagster.materialize()` in one Python process —
  avoids ~10-25s of `dg` CLI/project-load overhead per partition a bash loop
  of `dg launch` calls would pay every time). (2026-08-24)

## Deployment — waiting

- Serverless agent sync after PEX upload takes several minutes — normal.
  Don't poll tightly; `sleep 60` between checks. Success from the deploy
  command != loaded — confirm with `dg api` / status `LOADED`. (2026-08-24)
- `deploy-python-executable --build-method local` (PEX); Docker fallback
  (`serverless deploy` minus `--build-method`) if PEX fails on a
  source-only dependency. (2026-08-23)
- `deploy-python-executable --package-name <pkg>` fails to load ("No
  Definitions... found") for a `create-dagster`-scaffolded project — the
  `Definitions` object lives in `<pkg>.definitions`, not the top-level
  package. Use `--module-name <pkg>.definitions` instead. (2026-08-24)
- `dagster-cloud deployment delete-location` takes the location as a
  **positional arg**, not `--location-name` (that flag doesn't exist and
  errors). `deployment list-locations` prints names/images only, no status —
  for actual load status use `dg api code-location list --json` (has a real
  `status` field: `LOADED`/`FAILED`/etc.), matching what deployed locations'
  `code_source.module_name` should look like (`<pkg>.definitions`).
  (2026-08-24)
- A long-running `dagster-cloud serverless deploy-python-executable` call
  gets killed by a 120s shell-tool timeout while it's polling internally for
  agent sync (exit 143) even though the deploy itself keeps proceeding
  server-side — run it as an explicit background task, don't rely on the
  tool's auto-backgrounding for a command that prints periodic progress
  lines. (2026-08-24)

## Component schemas

- A component base may be a `@dataclass` `Resolvable`, not `dg.Model`
  (e.g. `DbtProjectComponent`). Adding a field via plain `pydantic.Field`
  is silently invisible in `--defs-yaml-schema` for these — use `@dataclass`
  on the subclass and `Annotated[T, dg.Resolver.default(description=...)] =
  default`. Check `ClassName.__mro__`/`model_fields` first. (2026-08-24)
- Multiple instances of the same dbt-project component (via `select`) need
  an explicit distinct `op: {name: ...}` — left to the default (auto-suffixed
  "_2", "_3"), Dagster cross-wires dependencies between instances ("op X
  does not have input Y" for a Y belonging to a different instance). Also
  collide on `defs_state` key (`DuplicateDefsStateKeyWarning`) — harmless
  (state is genuinely shared), not fixed. A dbt test referencing models
  split across two instances breaks the same way — keep cross-model tests
  in one instance. (2026-08-24)
- dbt generic test args (`accepted_values: {values: [...]}`, `relationships:
  {to:..., field:...}`) must nest under `arguments:` as of dbt-core 1.11, or
  parsing warns `MissingArgumentsPropertyInGenericTestDeprecation`. (2026-08-24)
- `MultiPartitionKey` stringifies dimensions **alphabetically by name**
  regardless of declaration order (`{"date":d,"carrier":c}` → `"<c>|<d>"`).
  Use `str(context.partition_key)`; don't hand-build the string. (2026-08-24)

## Automation conditions

- `AutomationCondition`'s `&`/`|`/`~` is Python-only — a Jinja `{{ }}` with
  `&` in defs.yaml raises `TemplateSyntaxError`. Compose it in a
  `template_vars_module` (`@dg.template_var` functions) and reference the
  named var in YAML. Use `template_vars_module: .foo` (dot-prefixed, no
  `.py`) — a literal `foo.py` is treated as an absolute import and raises
  `ModuleNotFoundError`. (2026-08-24)
- `AutomationCondition.all_deps_blocking_checks_passed()` = one-call shortcut
  for "eager, but wait for upstream blocking checks to pass." Load-bearing
  for any recovery-sequence demo — bare `eager()` fires even while the
  upstream's blocking check is still failing. (2026-08-24)
- dbt component `cli_args` Jinja scope for the partition is
  `partition_time_window` directly, not `context.partition_time_window`
  (`context` isn't bound there). A `cli_args` item needing a flag+value
  (`--vars`) must be a single-key YAML mapping (`- --vars: {min_date:...}`),
  not a hand-built JSON string — Dagster serializes it itself. (2026-08-24)

## Runtime gotchas

- `from __future__ import annotations` in a module with `@dg.asset`/
  `@dg.multi_asset` functions breaks context-parameter validation (the
  annotation becomes a literal string Dagster doesn't recognize). Don't use
  future annotations in files defining asset functions. (2026-08-24)
- `dg.AssetSpec(key="a/b")` in Python does NOT split "/" into path segments
  (unlike YAML `key: a/b`) — use `dg.AssetKey(["a","b"])`. (2026-08-24)
- DuckDB connections report `type(conn).__module__ == "_duckdb"` (leading
  underscore) — use `"duckdb" in module`, not `.startswith("duckdb")`.
  DuckDB allows one writer per file; wrap `duckdb.connect()` in a
  retry-with-backoff loop since Dagster's multiprocess executor can open the
  same file from sibling processes within milliseconds. (2026-08-24)
- `CREATE TABLE ... AS SELECT * FROM <empty pandas df>` in DuckDB can
  silently infer the WRONG column type (VARCHAR→INT32 seen) when a table's
  first-ever write is a 0-row frame. Pin the schema explicitly if a table's
  first write can legitimately be empty (a planted anomaly). (2026-08-24)
- Calling `dbt.cli()` twice in one `execute()` with the SAME injected
  `DbtCliResource` intermittently fails "No dbt_project.yml found" (dbt
  preps an isolated working copy per call; reusing the resource object
  races). Construct a fresh `DbtCliResource(dbt.project_dir)` for any extra
  untracked call. (2026-08-24)
- Without `DAGSTER_HOME` set, cross-run event-log state doesn't reliably
  persist between separate `dg` CLI invocations in local validation. (2026-08-24)

## Commands and flags

- Briefs and `state/ledger.json` live on `main` only. Use `dg`, never legacy
  `dagster` (`dg launch --assets`, not `--select`). `dagster-component init`
  does not scaffold a project — run `create-dagster project` first, always
  with `--auto-install`. `dg list components` empty → re-run
  `dagster-component init --force`. `components/__init__.py` must re-export
  each component class or the UI Components tab won't list them. (2026-08-23/24)

## Environment

- `GH_TOKEN` reads as literal `proxy-injected` when the proxy handles auth —
  treat as unset. Gmail has `create_draft` but no send. Cloud env vars are
  session-shell only, not visible to setup scripts. (2026-08-23)

## Registry gaps

- No component for generic REST/carrier-rate APIs (searched "rest api",
  "http polling", "rate limit", "carrier", "freight", "webhook source",
  "generic api"). Wrote custom `CarrierRateFeedComponent`/
  `ShipmentEventsComponent`. Worth contributing to the registry. (2026-08-24)

## Dead ends — don't retry these

- Do not put `dbt_project/` at the project root — it will not ship.
- Do not rely on `.gitignore`d build artifacts being present at runtime.
- Do not assume a local-file warehouse persists across separate Dagster+
  Serverless runs (see platform-constraint section above).
