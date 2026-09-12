"""Demo-mode subclass of the community-registry `AzureDataFactoryComponent`.

Represents the single most load-bearing directive in Umicore's brief:
Stonebranch (+ Ansible) stays master for infra-deployment scheduling, and the
one legacy Azure Data Factory pipeline it triggers is represented as an
**external, observed-not-triggered** asset -- exactly the "a Stonebranch job
can be represented and observed as an asset inside Dagster's lineage graph"
line from the AE's own business case (reportedly demonstrated live on the
August 5 call; this build makes that same moment repeatable).

`dagster-component search "azure data factory" --json` finds
`azure_data_factory` (`dagster_community_components.AzureDataFactoryComponent`),
a rung-2 registry component already following most of the workspace-component
convention: `@public` class, a `translation:` field, a `@public
get_asset_spec(props)` hook, `StateBackedComponent` inheritance with discovery
in `write_state_to_path`, and `polling_sensor` defaulting **True** (confirmed
by reading the field default in `component.py` -- do not assume off, per
house rules).

Same two registry gaps first recorded for demos/rvu-tempcover
(`component-feedback/2026-09-03-azure-data-factory-demo-mode-seam.md`), not
re-derived here since the same real component + same defect applies:

1. **No execution seam.** The pipeline-run-trigger-and-poll logic is inlined
   in the private module function `_build_adf_defs`, which calls the private
   free function `_get_adf_client(...)` directly. There is no method to
   override, so the seam is a process-lifetime monkeypatch of that free
   function -- the same "fake only the outermost network call" pattern
   `templates/demo_mode_pattern.py` calls for, just applied via monkeypatch
   because the component exposes no override point.
2. **The observation sensor doesn't respect `assets_by_pipeline_name`'s `key:`
   override** -- worked around the same way: this project's
   `defs/legacy_orchestration/defs.yaml` doesn't override the pipeline's
   default key, so the sensor's re-derived key and the asset's real key stay
   identical.

Crucially, this pipeline is configured **materializable but never
scheduled or triggered by Dagster** -- Dagster only observes it (via the
polling sensor) and consumes it downstream (via `deps:` from the corporate
rollup dbt layer). The brief is explicit: "observed -- never triggered."
Nothing in this project's automation conditions or schedules targets this
asset.
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from pydantic import Field

from umicore.components.azure_data_factory import component as _adf_module
from umicore.components.azure_data_factory.component import AzureDataFactoryComponent
from umicore.demo_data.adf_legacy_runs import list_legacy_adf_runs

_DEMO_PIPELINE_NAME = "stonebranch_nightly_mesh_sync"
_DEMO_RUN_ID_PREFIX = "demo-adf-run-"


class _DemoRunResult:
    """Stands in for the azure-mgmt-datafactory SDK's `PipelineRun` /
    `CreateRunResponse` model shapes -- only the attributes `_build_adf_defs`
    actually reads."""

    def __init__(self, run_id: str, status: str = "Succeeded", duration_seconds: float = 310.0):
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
        """Backs the observation sensor: reports Umicore's own Stonebranch/ADF
        run history for the window the sensor asks about, standing in for
        Stonebranch's own trigger log the way a real ADF Monitor tab would --
        Dagster reads it, Dagster never writes it."""
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
    object every network-crossing call in `_build_adf_defs` goes through."""

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

    Represents the one Stonebranch-owned nightly Azure Data Factory pipeline
    Umicore's coexistence story is built around -- the legacy side of the
    graph, shown side by side with the new Dagster-owned business-unit +
    dbt layer, never chained into it as a Dagster-triggered dependency.
    """

    demo_mode: bool = Field(
        default=True,
        description=(
            "Build a fixed one-pipeline workspace and simulate pipeline runs instead of "
            "calling the Azure Data Factory Management API. Set false and supply real "
            "subscription_id/resource_group_name/factory_name (+ Service Principal env "
            "vars, or omit for DefaultAzureCredential) in `workspace:` to run against a "
            "live ADF factory."
        ),
    )

    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)
        if self.demo_mode:
            _adf_module._get_adf_client = _demo_get_adf_client

    async def write_state_to_path(self, state_path: Path) -> None:
        if not self.demo_mode:
            return await super().write_state_to_path(state_path)

        state: dict[str, Any] = {
            "pipelines": [
                {
                    "name": _DEMO_PIPELINE_NAME,
                    "description": (
                        "Nightly ADF pipeline, triggered by Stonebranch, copying raw "
                        "business-unit extracts into the legacy Azure staging tables -- "
                        "the incumbent infra-scheduling investment this demo coexists "
                        "with rather than replaces."
                    ),
                    "parameters": ["runDate"],
                    "activities_count": 4,
                }
            ],
            "triggers": [],
            "linked_services": [],
            "datasets": [],
            "data_flows": [],
            "integration_runtimes": [],
        }
        state_path.write_text(json.dumps(state, indent=2))
