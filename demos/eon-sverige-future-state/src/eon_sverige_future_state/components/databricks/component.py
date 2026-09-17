"""Rung-3 subclass of the native `dagster_databricks.DatabricksWorkspaceComponent`.

Adds a `demo_mode: bool` field. Two overrides:

1. `write_state_to_path` — when `demo_mode`, writes a hardcoded list of
   `DatabricksJob` records for the E.ON Sverige demo instead of calling
   `self.workspace.fetch_jobs()` (which requires real Databricks credentials).
2. `_create_job_asset_def` — when `demo_mode`, wraps the multi_asset in a
   no-op execution body that yields `MaterializeResult` per selected key,
   skipping the real `client.jobs.run_now()` / poll cycle that would 401
   without valid credentials.

Everything else — `assets_by_job_task_key` schema, `build_defs`,
`build_defs_from_state`, `defs_state_config` — inherits from upstream
unchanged.

Real mode (`demo_mode: false`) hits E.ON's real Databricks workspace via
`workspace.host` + `workspace.token` and the multi_asset's execution body
submits + polls each job as designed by the native component.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    MaterializeResult,
    MetadataValue,
    multi_asset,
)
from dagster._serdes import serialize_value
from dagster_databricks import DatabricksWorkspaceComponent
from dagster_databricks.components.databricks_asset_bundle.configs import (
    DatabricksJob,
    DatabricksNotebookTask,
)
from dagster_databricks.components.databricks_workspace.component import (
    DatabricksWorkspaceData,
)


def _demo_notebook_task(job_name: str, task_key: str, notebook_path: str) -> DatabricksNotebookTask:
    return DatabricksNotebookTask(
        task_key=task_key,
        task_config={"notebook_task": {"notebook_path": notebook_path}},
        task_parameters={},
        depends_on=[],
        job_name=job_name,
        libraries=[],
    )


def _demo_jobs() -> list[DatabricksJob]:
    """The four E.ON Databricks jobs the demo represents.

    Real deployment against E.ON's workspace would return whatever
    `workspace.fetch_jobs()` produces from a live API call — same record shape.
    """
    return [
        DatabricksJob(
            job_id=1001,
            name="ingest_meter_reads_to_bronze",
            tasks=[_demo_notebook_task(
                job_name="ingest_meter_reads_to_bronze",
                task_key="main",
                notebook_path="/Shared/eon/ingest/meter_reads_to_bronze",
            )],
        ),
        DatabricksJob(
            job_id=1002,
            name="ingest_grid_telemetry_to_bronze",
            tasks=[_demo_notebook_task(
                job_name="ingest_grid_telemetry_to_bronze",
                task_key="main",
                notebook_path="/Shared/eon/ingest/grid_telemetry_to_bronze",
            )],
        ),
        DatabricksJob(
            job_id=1003,
            name="ingest_customer_records_to_bronze",
            tasks=[_demo_notebook_task(
                job_name="ingest_customer_records_to_bronze",
                task_key="main",
                notebook_path="/Shared/eon/ingest/customer_records_to_bronze",
            )],
        ),
        DatabricksJob(
            job_id=1004,
            name="bronze_data_quality_gate",
            tasks=[_demo_notebook_task(
                job_name="bronze_data_quality_gate",
                task_key="main",
                notebook_path="/Shared/eon/quality/bronze_dq_gate",
            )],
        ),
    ]


@dataclass
class EonDatabricksWorkspaceComponent(DatabricksWorkspaceComponent):
    """Native DatabricksWorkspaceComponent + demo-mode seam. See module docstring."""

    demo_mode: bool = False

    async def write_state_to_path(self, state_path: Path) -> None:
        if self.demo_mode:
            data = DatabricksWorkspaceData(jobs=_demo_jobs())
            state_path.write_text(serialize_value(data), encoding="utf-8")
            return
        await super().write_state_to_path(state_path)

    def _create_job_asset_def(
        self, job: DatabricksJob, specs: list[Any], task_key_map: dict[AssetKey, str]
    ) -> AssetsDefinition:
        if not self.demo_mode:
            return super()._create_job_asset_def(job, specs, task_key_map)

        asset_name = f"databricks_job_{job.job_id}"
        job_id = job.job_id
        job_name = job.name

        @multi_asset(name=asset_name, specs=specs, can_subset=True)
        def _demo_execution_fn(context: AssetExecutionContext):
            context.log.info(
                f"[demo mode] Skipping real Databricks submit for job "
                f"{job_id} ({job_name}). Real mode would call "
                f"client.jobs.run_now(job_id={job_id}) and poll to completion."
            )
            for spec in specs:
                if spec.key in context.selected_asset_keys:
                    task_key = task_key_map.get(spec.key, "unknown")
                    yield MaterializeResult(
                        asset_key=spec.key,
                        metadata={
                            "databricks/job_id": job_id,
                            "databricks/job_name": job_name,
                            "databricks/task_key": task_key,
                            "demo_mode": MetadataValue.text(
                                "simulated -- set demo_mode: false and supply "
                                "DATABRICKS_HOST + DATABRICKS_TOKEN for real execution"
                            ),
                        },
                    )

        return _demo_execution_fn
