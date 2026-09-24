# SsisWorkspaceComponent — four gaps found subclassing for demos/marketgrader

**What was needed:** Represent MarketGrader's still-running SQL Agent/SSIS
legacy estate (two packages, standing in for "every index not yet migrated")
as external, observe-only Dagster assets, with a demo-mode seam (no real SQL
Server credentials exist) and a flat, brief-specified asset key
(`legacy_sql_agent_price_ingest` / `legacy_sql_agent_index_calc`) rather than
the component's own `[ssis, <host>, <folder>, <project>, <package>]` scheme.

**What was searched** (all run with `--json`, from `demos/marketgrader`):

```
uvx --from dagster-community-components-cli dagster-component search "iceberg" --json
uvx --from dagster-community-components-cli dagster-component search "polaris catalog" --json
uvx --from dagster-community-components-cli dagster-component search "pyiceberg" --json
uvx --from dagster-community-components-cli dagster-component search "sql server" --json
uvx --from dagster-community-components-cli dagster-component search "mssql" --json
uvx --from dagster-community-components-cli dagster-component search "ssis" --json
uvx --from dagster-community-components-cli dagster-component search "sql agent" --json
```

`"sql server"` and `"ssis"` both surfaced `ssis_workspace` — "SQL Server
Integration Services (SSIS) as a Dagster workspace. Discovers
SSISDB.catalog.packages... Fivetran-shape workspace: block for connection +
selector for filtering + polling_sensor... + freshness_lag_threshold_seconds
asset check per package." A strong fit for the domain — used as-is at rung 2/3
(subclassed only for the demo-mode seam and the two gaps below), never
replaced.

**What came closest:** `ssis_workspace` itself — no rejection, no fallback to
a custom component. Read in full (`component.py`, 912 lines) before
subclassing.

**Update, same day:** after the initial observe-only build, the demo's
scope changed live (Eric, its intended user, reviewing the build) to
"observe AND kick off downstream workloads... take that over from the SQL
Agent... add the demo mode to fake it working" — i.e. `action: execute`
instead of `action: noop`, with the demo-mode fake extended to cover the
`create_execution`/`start_execution`/poll SQL too. That flip surfaced a
fourth gap (#4 below), invisible under `action: noop` because nothing ever
actually ran the check that gap lives in.

**What was built instead:** `DemoSsisWorkspaceComponent`
(`demos/marketgrader/src/marketgrader/components/demo_ssis_workspace.py`), a
subclass overriding exactly four things:

1. `_build_engine()` — the component's one genuine network-boundary seam;
   swapped for `FakeSsisEngine` (`demo_data/legacy_ssis.py`) in demo mode.
   Every other method (`_discover_packages`, the sensor's completion poll,
   the freshness check) runs completely unmodified against it. This part
   worked exactly as the workspace-component convention promises — no gap.

2. **Gap: no `partitions_def` field at all.** The base translator's
   `get_asset_spec` builds an unpartitioned `AssetSpec` unconditionally, and
   the observation sensor's `AssetObservation`s carry no `partition=`
   argument — there's no way to report per-partition legacy execution
   history even if the caller wanted to. Not worked around for this build
   (the legacy assets are observed standalone here, never chained into a
   partitioned pipeline), but would block a prospect whose legacy packages
   genuinely run per-day/per-region and want that reflected.

3. **Gap: the observation sensor ignores `get_asset_spec`'s key override.**
   `_build_observation_sensor` independently reconstructs
   `[*prefix, folder_name, project_name, pkg_name]` for each
   `AssetObservation`'s key, instead of calling `self.get_asset_spec(props).key`
   — the exact hook the component's own docstring calls "the documented
   override point for customizing asset keys." A subclass that overrides
   `get_asset_spec` (as `templates/demo_mode_pattern.py` / rung 3 explicitly
   recommends for "asset keys come from the source system, not us") silently
   breaks sensor/asset identity: observations land on a key nothing in the
   graph has, and the UI shows no activity on the actual asset. Same shape
   as the Azure Data Factory finding recorded for demos/rvu-tempcover
   (`component-feedback/2026-09-03-azure-data-factory-demo-mode-seam.md`).
   Worked around by overriding `_build_observation_sensor` to build the key
   via `get_asset_spec` instead, and locked in by `validate_e2e.py`'s
   "sensor and asset agree on identity" assertion (confirmed the gap is
   real: reverting the override reproduces `KeyError`-shaped observations on
   a key not in the graph).

4. **Gap: `freshness_lag_threshold_seconds`'s check can't be evaluated
   through a job when `action: noop`.** `build_defs_from_state` builds
   `assets = list(specs)` (bare `AssetSpec`s, zero compute) for
   `action: noop`. Calling
   `Definitions.resolve_implicit_job_def_def_for_assets([key])` on a
   check-only selection over one of these keys raises
   `DagsterInvalidDefinitionError: Selected keys must be a subset of
   existing executable asset keys` — Dagster can't build a job with no
   executable node in it. This isn't demo_mode-specific; it would bite a
   real `action: noop` deployment identically. Worked around in
   `validate_e2e.py` by invoking the `AssetChecksDefinition` directly via
   `dg.build_asset_check_context` rather than through a job — works, but
   isn't how a user would discover this in `dg dev`'s "materialize" button
   (which also can't target a check with no compute to attach to).

5. **Gap: `_build_freshness_checks`'s per-package closure trick breaks
   under `action: execute`.** The parent's `_check` function is defined as
   `def _check(_spec_key=spec.key): ...` inside `for spec in specs:` — a
   default-argument trick to dodge Python's late-binding closure bug (every
   `_check` closure would otherwise see the *last* loop's `spec`). But
   Dagster's asset-check machinery treats any non-`context` parameter on an
   `@asset_check` function as an input to load from the checked asset via
   the project's default IO manager. Under `action: noop` this was
   invisible — gap #4 above meant the check never actually ran through a
   job. Once `action: execute` made the assets genuinely executable and
   this demo's default `io_manager` is type-specific (only handles
   `pyarrow.Table`/`RecordBatchReader`, for the real Iceberg IO manager
   backing the *other* assets in the project), every freshness-check run
   failed with `CheckError: IcebergIOManager does not have a handler for
   type 'typing.Any'` — a check with nothing to do with Iceberg, broken by
   an unrelated resource elsewhere in the same project. Worked around by
   overriding `_build_freshness_checks` with a factory-function closure
   (`_make_check(spec)`, `spec` a real parameter) instead of the
   default-argument trick — the correct fix for the late-binding bug, and
   it also means the check function takes only `context`, so Dagster infers
   no input at all.

**Suggested changes** (smallest edits that would have closed each gap):

- Add an optional `partitions_def:` field to `SsisWorkspaceComponent`,
  threaded into `SsisComponentTranslator.get_asset_spec` and into the
  sensor's `AssetObservation(partition=...)` construction.
- In `_build_observation_sensor`, replace the manual
  `[*prefix, folder_name, project_name, pkg_name]` key construction with a
  call to `self.get_asset_spec(props).key`, matching what
  `build_defs_from_state` already does for the primary spec list. One-line
  fix, and it makes every subclass's `get_asset_spec` override trustworthy
  by construction instead of by convention.
- Either document that `freshness_lag_threshold_seconds` checks on
  `action: noop` packages are only evaluable outside a job (and how), or
  give `build_defs_from_state` a check-only executable stub for `noop`
  packages so `dg launch --assets <key> --select-checks <name>`-shaped
  workflows work the same as they do for `action: execute`.
- In `_build_freshness_checks`, replace the `def _check(_spec_key=spec.key):`
  default-argument trick with a factory function that takes `spec` as a
  real parameter and defines `_check(context)` inside it — closes over
  `spec` correctly without introducing a phantom Dagster input. Also worth
  a regression test that materializes an `action: execute` package in a
  project whose default `io_manager` is type-specific, so this doesn't
  regress silently again.
