"""Demo-mode subclass of the community-registry `AzureDataFactoryComponent`.

Fixes the defect this rebuild exists to correct (see
`requests/done/rvu-tempcover-*.md`): the prior two rebuilds gave Fivetran
and Power BI real, demo-mode-subclassed components but left Azure Data
Factory -- the incumbent the whole demo thesis is built against ("ADF can
tell you a job ran; this shows whether the data was right") -- as prose
only, with no asset in the graph at all. `dagster-component search "azure
data factory" --json` finds `azure_data_factory`
(`dagster_community_components.AzureDataFactoryComponent`), a rung-2
registry component already following most of the workspace-component
convention: `@public` class, a `translation:` field, a `@public
get_asset_spec(props)` hook, `StateBackedComponent` inheritance with
discovery in `write_state_to_path`, and (unusually -- see LEARNINGS.md)
`polling_sensor` defaulting **True**.

Two gaps the real component does not cover, both worth a
`component-feedback/` entry rather than silently working around:

1. **No execution seam.** Unlike `FivetranAccountComponent` (`execute()`)
   or `PowerBIWorkspaceComponent`
   (`build_semantic_model_refresh_asset_definition()`), this component's
   pipeline-run-trigger-and-poll logic is inlined as a closure inside the
   private module function `_build_adf_defs`, which calls the private
   free function `_get_adf_client(...)` directly rather than through
   `self.workspace.get_client()`. There is no method to override. The
   smallest fix that preserves every other real code path (spec
   construction, partitions, freshness, retry policy, the sensor's own
   filtering) is to substitute that one free function for the duration of
   this process, which is exactly the "fake only the outermost network
   call" seam `templates/demo_mode_pattern.py` calls for -- just applied
   via monkeypatch instead of an override method, because the component
   doesn't expose one. Suggested upstream fix: extract the pipeline
   trigger-and-poll body into an overridable
   `execute_pipeline_run(self, adf_client, pipeline_name, parameters)`
   method, matching the Fivetran/Power BI convention.
2. **`translation:` / `get_asset_spec()` never fires for pipelines.** Both
   are only wired into the four *untested* external-asset kinds
   (`_emit_external_assets`); the pipeline multi_asset's spec is built
   directly by `_build_adf_defs` and bypasses the translator entirely.
   The only supported per-pipeline customization is the legacy
   `assets_by_pipeline_name` override dict, which is what this project
   uses in `defs/legacy_orchestration/defs.yaml`. Suggested upstream fix:
   route pipeline spec construction through `get_asset_spec()` too, so
   `translation:` and subclass overrides work uniformly across every
   object kind.
3. **The observation sensor doesn't respect `assets_by_pipeline_name`'s
   `key:` override.** `adf_observation_sensor` builds each
   `AssetMaterialization`'s `asset_key` straight from the raw ADF pipeline
   name (`f"adf_pipeline_{run_pipeline_name}"`), not from the overridden
   spec key -- confirmed by materializing this project's pipeline asset
   under an overridden key and then evaluating the sensor directly, which
   emitted an observation against the *unoverridden* default key instead
   (a dangling observation that would never attach to the visible asset in
   the UI). Worked around here by **not** overriding the key at all --
   `defs/legacy_orchestration/defs.yaml` only overrides `description` and
   `metadata`, so the pipeline's default key
   (`adf_pipeline_<pipeline_name>`) is exactly what both the asset and the
   sensor use. Suggested upstream fix: have the sensor look up each
   pipeline's actual spec key (via the same `assets_by_pipeline_name` /
   future `get_asset_spec()` path) instead of re-deriving it.

The seam here is `_get_adf_client` (patched once, at demo_mode-instance
construction, for the process's lifetime -- the returned sensor/asset
closures resolve that name from the module's globals *at call time*, so a
scoped patch-and-restore around a single method call would already have
been reverted by the time the sensor or a later run actually invokes it).
This project only ever runs one Azure Data Factory component instance, in
one mode, so a process-lifetime patch is safe. Nothing else changes:
`write_state_to_path` still runs through the real state-write path (no
network at Dagster load time, only a fixed pipeline list instead of a live
`client.pipelines.list_by_factory()` call), and `_build_adf_defs` --
spec construction, partitions, retry policy, sensor structure -- is the
exact same, unmodified real code, per Rule 1 of
`templates/demo_mode_pattern.py`.
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from pydantic import Field

from rvu_tempcover.components.azure_data_factory import component as _adf_module
from rvu_tempcover.components.azure_data_factory.component import AzureDataFactoryComponent
from rvu_tempcover.demo_data.adf_legacy_runs import list_legacy_adf_runs

_DEMO_PIPELINE_NAME = "legacy_nightly_ingestion"
_DEMO_RUN_ID_PREFIX = "demo-adf-run-"


class _DemoRunResult:
    """Stands in for the azure-mgmt-datafactory SDK's `PipelineRun` /
    `CreateRunResponse` model shapes -- only the attributes `_build_adf_defs`
    actually reads.
    """

    def __init__(self, run_id: str, status: str = "Succeeded", duration_seconds: float = 245.0):
        self.run_id = run_id
        self.status = status
        self.message = None
        now = datetime.utcnow()
        self.run_start = now - timedelta(seconds=duration_seconds)
        self.run_end = now


class _DemoRunListResult:
    def __init__(self, value: list):
        self.value = value


class _DemoPipelinesOps:
    def create_run(self, resource_group_name, factory_name, pipeline_name, **kwargs):
        return _DemoRunResult(run_id=f"{_DEMO_RUN_ID_PREFIX}{pipeline_name}")

    def list_by_factory(self, resource_group_name, factory_name):
        return []


class _DemoPipelineRunsOps:
    def get(self, resource_group_name, factory_name, run_id):
        return _DemoRunResult(run_id=run_id)

    def query_by_factory(self, resource_group_name, factory_name, filter_params):
        """Backs the observation sensor: reports RVU's own ADF-side run
        history for the window the sensor asks about, standing in for
        `list_legacy_adf_runs()` the way a carrier's own API would --
        Dagster reads it, Dagster never writes it (CLAUDE.md, "Assets are
        idempotent -- the source changes, not the asset").
        """
        # `RunFilterParameters` (a real Azure SDK/msrest model) silently
        # coerces naive datetimes to UTC-aware ones on assignment, so the
        # fixture's own naive timestamps have to be made UTC-aware here too
        # or every comparison below raises `TypeError: can't compare
        # offset-naive and offset-aware datetimes`.
        after = getattr(filter_params, "last_updated_after", None)
        before = getattr(filter_params, "last_updated_before", None)
        rows = []
        for run in list_legacy_adf_runs():
            run_end = datetime.fromisoformat(run["run_end"]).replace(tzinfo=timezone.utc)
            if after and run_end < after:
                continue
            if before and run_end > before:
                continue
            rows.append(_DemoLegacyRun(run))
        return _DemoRunListResult(rows)


class _DemoLegacyRun:
    def __init__(self, row: dict):
        self.run_id = row["run_id"]
        self.status = row["status"]
        self.pipeline_name = row["pipeline_name"]
        self.run_start = datetime.fromisoformat(row["run_start"]).replace(tzinfo=timezone.utc)
        self.run_end = datetime.fromisoformat(row["run_end"]).replace(tzinfo=timezone.utc)
        self.message = row.get("message")


class _DemoActivityRunsOps:
    def query_by_pipeline_run(self, resource_group_name, factory_name, run_id, filter_params):
        return _DemoRunListResult([])


class _DemoTriggersOps:
    def list_by_factory(self, resource_group_name, factory_name):
        return []


class _DemoTriggerRunsOps:
    def query_by_factory(self, resource_group_name, factory_name, filter_params):
        return _DemoRunListResult([])


class _DemoAdfClient:
    """Fakes `azure.mgmt.datafactory.DataFactoryManagementClient` -- the one
    object every network-crossing call in `_build_adf_defs` goes through.
    """

    def __init__(self):
        self.pipelines = _DemoPipelinesOps()
        self.pipeline_runs = _DemoPipelineRunsOps()
        self.activity_runs = _DemoActivityRunsOps()
        self.triggers = _DemoTriggersOps()
        self.trigger_runs = _DemoTriggerRunsOps()


def _demo_get_adf_client(subscription_id, tenant_id, client_id, client_secret):
    return _DemoAdfClient()


class DemoAzureDataFactoryComponent(AzureDataFactoryComponent):
    """`AzureDataFactoryComponent` with a demo-mode discovery + execution seam.

    Represents RVU's actual incumbent: the one ADF pipeline doing double
    duty as "orchestration" today (per the brief's stack table), with none
    of the per-asset checks, freshness policies, or lineage the
    Fivetran+dbt pipeline downstream carries -- the literal contrast the
    demo name ("From Ran to Right") makes on screen.
    """

    demo_mode: bool = Field(
        default=True,
        description=(
            "Build a fixed one-pipeline workspace and simulate pipeline runs "
            "instead of calling the Azure Data Factory Management API. Set "
            "false and supply real subscription_id/resource_group_name/"
            "factory_name (+ Service Principal env vars, or omit for "
            "DefaultAzureCredential) in `workspace:` to run against a live "
            "ADF factory."
        ),
    )

    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)
        if self.demo_mode:
            # Process-lifetime patch -- see module docstring for why this
            # can't be a scoped patch-and-restore around one call. Only
            # ever applied when demo_mode is true; real mode never touches
            # `_adf_module._get_adf_client`.
            _adf_module._get_adf_client = _demo_get_adf_client

    async def write_state_to_path(self, state_path: Path) -> None:
        """The discovery seam. Real mode calls the live ADF Management API
        via `client.pipelines.list_by_factory(...)`. Demo mode writes the
        identical state-file shape (see `AzureDataFactoryComponent.
        write_state_to_path`) from one fixed pipeline description instead,
        so `build_defs_from_state` -- spec construction, `assets_by_
        pipeline_name` overrides, partitions, retry policy, sensor
        structure -- runs completely unmodified downstream.
        """
        if not self.demo_mode:
            return await super().write_state_to_path(state_path)

        state: dict[str, Any] = {
            "pipelines": [
                {
                    "name": _DEMO_PIPELINE_NAME,
                    "description": (
                        "Nightly ADF pipeline copying quote, policy, panel, and "
                        "partner data from source systems into the legacy Azure "
                        "warehouse staging tables -- the pipeline this rebuild "
                        "replaces."
                    ),
                    "parameters": ["runDate"],
                    "activities_count": 6,
                }
            ],
            "triggers": [],
            "linked_services": [],
            "datasets": [],
            "data_flows": [],
            "integration_runtimes": [],
        }
        state_path.write_text(json.dumps(state, indent=2))
