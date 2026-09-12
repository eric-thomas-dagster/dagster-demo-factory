"""Demo-mode subclass of the native `dagster_databricks.DatabricksWorkspaceComponent`.

Rung 1 of the escalation ladder: `dagster-databricks` ships this workspace
component natively (`import dagster_databricks; DatabricksWorkspaceComponent`),
following the same workspace-component convention read directly from its
source: `StateBackedComponent` inheritance with enumeration in
`write_state_to_path` (via the `workspace.fetch_jobs()` resource method, not
inline), `assets_by_job_task_key` explicit mapping (the
`assets_by_task_key`-shaped convention CLAUDE.md calls for), and a
`@public get_asset_specs(task, job_name)` override hook.

**Gap found (worth its own `component-feedback/` entry): no observation
surface at all.** Unlike `AzureDataFactoryComponent` (`polling_sensor`,
default True) or `PowerBIWorkspaceComponent`'s workspace-component siblings,
`DatabricksWorkspaceComponent` builds no sensor and has no
`polling_sensor` / `generate_sensor` field -- confirmed by reading
`dagster_databricks/components/databricks_workspace/component.py` in full
(no `sensor` identifier anywhere in the module). This component adds one,
per CLAUDE.md's "Observe, don't just execute" -- Umicore's whole coexistence
pitch depends on Dagster noticing a Databricks job someone (or Stonebranch)
ran outside it.

Two independent seams, both additive over the parent, neither touching
asset keys, specs, partitions, or the YAML schema:

1. **Discovery (`DemoDatabricksWorkspace.fetch_jobs`).** Real mode calls the
   live Databricks Jobs API via async HTTP, exactly as the parent resource
   does. Demo mode returns a fixed two-job list
   (`demo_data/databricks_jobs.py`) built through the same
   `parse_task_from_config` real code path the live fetch uses, so
   `write_state_to_path` (inherited, unmodified) and every downstream spec
   construction step is the real component's code.
2. **Execution (`_create_job_asset_def`).** The base component's execution
   closure is itself the network-crossing call -- there is no smaller
   method to override, the same situation the Azure Data Factory demo
   subclass documents (`azure_data_factory_demo.py`). Demo mode substitutes
   a closure with byte-identical specs/`can_subset` behaviour that writes
   deterministic stub rows into the shared DuckDB warehouse (standing in
   for the real PySpark job landing its output table) instead of calling
   `client.jobs.run_now(...)`. Real mode delegates to `super()`, unmodified.

The observation sensor (`generate_sensor`, opt-in, off by default --
matching the workspace-component convention's usual default, per
LEARNINGS.md's note that this varies by component) reads
`DemoDatabricksWorkspace.list_recent_runs()` -- demo mode returns a fixed
external-run fixture (`demo_data/databricks_runs.py`); real mode resolves
the job id and calls `client.jobs.list_runs(job_id=..., limit=25)`. Either
way it emits one `AssetObservation` per unseen terminal run against the
asset keys `assets_by_job_task_key` maps to that job, so a run Databricks'
own history shows that Dagster didn't trigger still shows up in the graph.
"""

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Any

import dagster as dg
from dagster import Resolvable
from dagster.components import Resolver
from dagster.components import ResolutionContext

from dagster_databricks.components.databricks_asset_bundle.configs import (
    DatabricksJob,
    DatabricksNotebookTask,
)
from dagster_databricks.components.databricks_asset_bundle.resource import DatabricksWorkspace
from dagster_databricks.components.databricks_workspace.component import DatabricksWorkspaceComponent

from umicore.demo_data.bu_stub_data import stub_dataframe
from umicore.demo_data.databricks_jobs import DEMO_JOBS, TABLE_BY_JOB_TASK, PARTITION_COLUMN_BY_TABLE
from umicore.demo_data.databricks_runs import list_external_databricks_runs
from umicore.demo_data.write_raw_table import write_raw_table


@dataclass
class DemoDatabricksWorkspaceArgs(Resolvable):
    """Aligns with `DemoDatabricksWorkspace.__init__` -- adds `demo_mode` to
    the base `DatabricksWorkspaceArgs` shape."""

    host: str
    token: str
    demo_mode: bool = True


def _resolve_demo_databricks_workspace(context: ResolutionContext, model) -> "DemoDatabricksWorkspace":
    args = DemoDatabricksWorkspaceArgs.resolve_from_model(context, model)
    return DemoDatabricksWorkspace(host=args.host, token=args.token, demo_mode=args.demo_mode)


class DemoDatabricksWorkspace(DatabricksWorkspace):
    """`DatabricksWorkspace` resource with a demo-mode discovery seam.

    Pydantic `ConfigurableResource`-based (like the parent), so a plain
    `Field()` default works here -- same shape as the Snowflake resource
    variant in `templates/demo_mode_pattern.py`.
    """

    demo_mode: bool = True

    async def fetch_jobs(self, databricks_filter: Any) -> list[DatabricksJob]:
        if not self.demo_mode:
            return await super().fetch_jobs(databricks_filter)

        jobs = []
        for job_cfg in DEMO_JOBS:
            task = DatabricksNotebookTask.from_job_task_config(
                {
                    "task_key": job_cfg["task_key"],
                    "job_name": job_cfg["job_name"],
                    "notebook_task": {"notebook_path": job_cfg["notebook_path"], "base_parameters": {}},
                }
            )
            jobs.append(DatabricksJob(job_id=job_cfg["job_id"], name=job_cfg["job_name"], tasks=[task]))
        return jobs

    def list_recent_runs(self, job_name: str) -> list[dict]:
        """Terminal run history for one job, normalized to
        `{run_id, result_state, run_end}` regardless of mode -- what the
        observation sensor reads."""
        if not self.demo_mode:
            client = self.get_client()
            matches = list(client.jobs.list(name=job_name))
            if not matches:
                return []
            job_id = matches[0].job_id
            runs = client.jobs.list_runs(job_id=job_id, limit=25)
            return [
                {
                    "run_id": str(run.run_id),
                    "result_state": (run.state.result_state.value if run.state and run.state.result_state else "UNKNOWN"),
                    "run_end": datetime.fromtimestamp(run.end_time / 1000, tz=timezone.utc).isoformat()
                    if getattr(run, "end_time", None)
                    else datetime.now(timezone.utc).isoformat(),
                }
                for run in runs
            ]
        return list_external_databricks_runs(job_name)


@dataclass
class DemoDatabricksWorkspaceComponent(DatabricksWorkspaceComponent):
    """`DatabricksWorkspaceComponent` with a demo-mode execution seam plus
    an opt-in observation sensor the base component doesn't have.

    `generate_sensor` defaults `False` -- following the workspace-component
    convention's usual off-by-default shape (LEARNINGS.md notes this varies
    per component; this one is off like Power BI's, not on like ADF's).
    """

    workspace: Annotated[
        DatabricksWorkspace,
        Resolver(
            _resolve_demo_databricks_workspace,
            model_field_type=DemoDatabricksWorkspaceArgs.model(),
            description="The mapping defining a DatabricksWorkspace, plus demo_mode.",
        ),
    ]

    generate_sensor: Annotated[
        bool,
        Resolver.default(
            description=(
                "Emit a polling sensor that observes Databricks job-run history and "
                "emits AssetObservation events for runs Dagster didn't trigger."
            )
        ),
    ] = False

    poll_interval_seconds: Annotated[
        int,
        Resolver.default(description="Sensor polling interval (seconds)."),
    ] = 60

    def _create_job_asset_def(self, job, specs, task_key_map):
        if not getattr(self.workspace, "demo_mode", False):
            return super()._create_job_asset_def(job, specs, task_key_map)

        asset_name = f"databricks_job_{job.job_id}"

        @dg.multi_asset(name=asset_name, specs=specs, can_subset=True)
        def _execution_fn(context: dg.AssetExecutionContext):
            partition_date = context.partition_key if context.has_partition_key else None
            selected_keys = context.selected_asset_keys
            for spec in specs:
                if spec.key not in selected_keys:
                    continue
                task_key = task_key_map.get(spec.key, "unknown")
                table_name = TABLE_BY_JOB_TASK.get((job.name, task_key))
                written = 0
                if table_name and partition_date:
                    frame = stub_dataframe(table_name, partition_date, row_count=60)
                    written = write_raw_table(
                        table_name, frame, PARTITION_COLUMN_BY_TABLE[table_name], partition_date
                    )
                yield dg.MaterializeResult(
                    asset_key=spec.key,
                    metadata={
                        "dagster/row_count": written,
                        "dagster-databricks/job_id": job.job_id,
                        "dagster-databricks/task_key": task_key,
                        "source": dg.MetadataValue.text(
                            "simulated Databricks job run -- set demo_mode: false on the "
                            "workspace: block to trigger the real job via the Databricks "
                            "Jobs API"
                        ),
                    },
                )

        return _execution_fn

    def build_defs_from_state(self, context: Any, state_path: Path | None) -> dg.Definitions:
        defs = super().build_defs_from_state(context, state_path)
        if not self.generate_sensor:
            return defs
        return dg.Definitions.merge(defs, dg.Definitions(sensors=[self._build_observation_sensor()]))

    def _build_observation_sensor(self) -> dg.SensorDefinition:
        job_asset_keys: dict[str, list[dg.AssetKey]] = {}
        for job_name, task_map in (self.assets_by_job_task_key or {}).items():
            keys: list[dg.AssetKey] = []
            for specs in task_map.values():
                keys.extend(spec.key for spec in specs)
            job_asset_keys[job_name] = keys
        workspace = self.workspace

        @dg.sensor(
            name="databricks_workspace_observation_sensor",
            minimum_interval_seconds=self.poll_interval_seconds,
        )
        def _sensor(context: dg.SensorEvaluationContext) -> dg.SensorResult:
            cursor = json.loads(context.cursor) if context.cursor else {}
            events: list[dg.AssetObservation] = []
            new_cursor: dict[str, list[str]] = {}
            for job_name, asset_keys in job_asset_keys.items():
                if not asset_keys:
                    continue
                seen = set(cursor.get(job_name, []))
                runs = workspace.list_recent_runs(job_name)
                run_ids = list(seen)
                for run in runs:
                    if run["run_id"] in seen:
                        continue
                    for asset_key in asset_keys:
                        events.append(
                            dg.AssetObservation(
                                asset_key=asset_key,
                                metadata={
                                    "dagster-databricks/job_name": job_name,
                                    "dagster-databricks/run_id": run["run_id"],
                                    "dagster-databricks/result_state": run["result_state"],
                                    "dagster-databricks/observed_run_end": run["run_end"],
                                    "source": dg.MetadataValue.text(
                                        "run detected in Databricks job-run history that "
                                        "Dagster did not trigger"
                                    ),
                                },
                            )
                        )
                    run_ids.append(run["run_id"])
                new_cursor[job_name] = run_ids[-20:]
            return dg.SensorResult(asset_events=events, cursor=json.dumps(new_cursor))

        return _sensor
