# Azure Data Factory component -- no execution seam, sensor ignores key overrides

**Date:** 2026-09-03
**Build:** `demos/rvu-tempcover` ("From Ran to Right") -- correction rebuild,
adding Azure Data Factory as a first-class component per
`requests/done/rvu-tempcover-2026-09-03.md` (two prior builds left ADF as
prose only, despite it being the incumbent the whole demo thesis argues
against).

## What was needed

Azure Data Factory represented as a real, materializing, observed Dagster
asset -- not a metadata label -- for the one legacy pipeline RVU's ADF
instance runs today, contrasted in the same graph against the new
Fivetran+dbt pipeline's per-asset checks. No real ADF instance or
credentials exist for this build, so a demo-mode I/O seam was required
per `templates/demo_mode_pattern.py`.

## What was searched

```
dagster-component search "azure data factory" --json
dagster-component info azure_data_factory
```

`azure_data_factory` (`dagster_community_components.AzureDataFactoryComponent`)
was the first and only hit searched for -- a strong, unambiguous match
(score 369, matched on id/name/description/tags), so no further searches
were needed to confirm it was the right rung-2 component before
subclassing.

## What came closest, and specifically why it didn't fit as-is

`azure_data_factory` already follows most of the workspace-component
convention: `@public` class, a `translation:` field, a `@public
get_asset_spec(props)` hook, `StateBackedComponent` inheritance with
discovery in `write_state_to_path`, and a `polling_sensor` field
(unusually defaulting **True** -- see `LEARNINGS.md`, since every other
workspace component checked so far defaults it False). Two gaps required
subclassing beyond a config-field tweak:

1. **No execution seam.** `FivetranAccountComponent` exposes `execute()`;
   `PowerBIWorkspaceComponent` exposes
   `build_semantic_model_refresh_asset_definition()`. This component's
   pipeline-trigger-and-poll body is inlined as a closure inside the
   private module function `_build_adf_defs`, which calls the private
   free function `_get_adf_client(...)` directly -- there is no override
   point. Confirmed by reading `component.py` end to end, not inferred.
2. **The observation sensor ignores `assets_by_pipeline_name`'s `key:`
   override.** `adf_observation_sensor` builds each
   `AssetMaterialization`'s `asset_key` as
   `f"adf_pipeline_{run_pipeline_name}"` -- the *raw* ADF pipeline name --
   rather than looking up the overridden spec key. Confirmed empirically,
   not by reading alone: materializing this project's pipeline asset
   under an overridden key and then evaluating the sensor directly
   produced an `AssetObservation` against the **unoverridden default
   key**, which would never attach to the visible asset in the Dagster
   UI. `validate_e2e.py` now asserts sensor-observation keys match the
   materializable asset's key specifically to catch a regression here.

Neither gap is disqualifying per CLAUDE.md's escalation ladder ("if it
gained two config fields, would it work?") -- both are workarounds, not
reasons to write a component from scratch.

## What was built instead

`src/rvu_tempcover/components/azure_data_factory_demo.py` --
`DemoAzureDataFactoryComponent(AzureDataFactoryComponent)`:

- `write_state_to_path` overridden for demo mode: writes a fixed
  one-pipeline state dict instead of calling
  `client.pipelines.list_by_factory(...)`. Real mode delegates to the
  parent unchanged.
- Gap 1 workaround: `model_post_init` monkeypatches the module-level
  `_get_adf_client` free function with a fake Azure SDK client
  (`_DemoAdfClient`, implementing only the `.pipelines` / `.pipeline_runs`
  / `.activity_runs` / `.triggers` / `.trigger_runs` surface
  `_build_adf_defs`'s closures actually call) -- for the process's
  lifetime when `demo_mode=True`, never touched when `demo_mode=False`.
  This is the smallest change that keeps every other real code path
  (spec construction, partitions, retry policy, filters, sensor
  structure) genuinely shared between demo and live mode, since there's
  no method-level seam to override instead.
- Gap 2 workaround: `defs/legacy_orchestration/defs.yaml` deliberately
  does **not** override the pipeline's `key:` (only `description` and
  `metadata`), and the demo pipeline is named `legacy_nightly_ingestion`
  so the component's own default key
  (`adf_pipeline_legacy_nightly_ingestion`) already reads cleanly.
- `src/rvu_tempcover/demo_data/adf_legacy_runs.py` backs the sensor: a
  fixed, deterministic run history (row identity fixed, timestamps
  relative to sensor-call time) standing in for what
  `pipeline_runs.query_by_factory` would return from ADF's own run-history
  API -- CLAUDE.md's "mock the source system, including arrival timing"
  applied to a legacy scheduler's run log instead of a data feed.

## Suggested changes

1. Extract the pipeline trigger-and-poll body out of `_build_adf_defs`'s
   closure into an overridable method, e.g.
   `execute_pipeline_run(self, adf_client, pipeline_name, parameters) ->
   PipelineRun`, matching the Fivetran/Power BI convention. Would remove
   the need for the `_get_adf_client` monkeypatch entirely.
2. Route pipeline spec construction through `get_asset_spec()` (or at
   minimum have `adf_observation_sensor` resolve the *actual* spec key for
   a given pipeline name, via the same `assets_by_pipeline_name` table it
   already reads) so the sensor and the asset agree on identity without a
   YAML-level workaround.
