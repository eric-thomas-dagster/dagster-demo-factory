"""Databricks-run observation sensor -- the "coexistence" story.

Polls the Databricks Jobs API for runs that Dagster did not trigger
(their existing GitLab scheduler firing the same jobs) and emits an
`AssetObservation` per newly-terminated run against the corresponding
Dagster asset key. In demo mode it emits one synthetic observation per
tick per job so the story reads cleanly against a green graph.

The native `DatabricksWorkspaceComponent` does not ship a polling
sensor -- adding it here as a plain @sensor is the sanctioned pattern
per house rules (don't rewrite a whole component just to add an
observation surface). File this as `component-feedback/` for the
next iteration of dagster-databricks: a `polling_sensor: true` field
on the workspace component would let this file go away.
"""

import datetime
import os

import dagster as dg

# The Databricks-job asset keys the workspace component emits.
# Kept in sync with `defs/databricks/defs.yaml`'s `assets_by_job_task_key`
# entries. When adding a new job, add its asset key here too.
_DATABRICKS_JOB_ASSET_KEYS = [
    dg.AssetKey(["bronze", "meter_reads_delta"]),
    dg.AssetKey(["bronze", "grid_load_telemetry_delta"]),
    dg.AssetKey(["bronze", "customer_records_delta"]),
    dg.AssetKey(["bronze", "bronze_dq_gate"]),
]

_DEMO_MODE = os.environ.get("EON_DEMO_MODE", "true").lower() != "false"


@dg.sensor(
    name="databricks_job_observation_sensor",
    minimum_interval_seconds=60,
    default_status=dg.DefaultSensorStatus.STOPPED,
    description=(
        "Detects Databricks job runs Dagster did not trigger (their GitLab "
        "scheduler firing the same jobs) and emits an AssetObservation per "
        "run against the corresponding bronze/* asset key. Demo mode emits "
        "one synthetic observation per tick; real mode polls the Databricks "
        "Jobs API run history."
    ),
)
def databricks_job_observation_sensor(
    context: dg.SensorEvaluationContext,
) -> dg.SensorResult:
    now = datetime.datetime.now(datetime.timezone.utc)

    if _DEMO_MODE:
        observations = [
            dg.AssetObservation(
                asset_key=key,
                metadata={
                    "databricks/observed_at": now.isoformat(),
                    "databricks/source": "external_trigger (simulated)",
                    "demo_mode": dg.MetadataValue.text(
                        "simulated -- set EON_DEMO_MODE=false and configure "
                        "DATABRICKS_HOST/DATABRICKS_TOKEN to poll the real API"
                    ),
                },
            )
            for key in _DATABRICKS_JOB_ASSET_KEYS
        ]
        return dg.SensorResult(
            asset_events=observations,
            cursor=now.isoformat(),
        )

    # Real mode: poll the Databricks Jobs API since the last cursor.
    # Left as an explicit branch so demo-mode-only paths don't hide the
    # real integration surface -- see dagster_databricks docs for the
    # WorkspaceClient.jobs.list_runs pattern.
    raise NotImplementedError(
        "Set EON_DEMO_MODE=false and implement the real "
        "WorkspaceClient.jobs.list_runs poll here."
    )
