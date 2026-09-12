"""History of Databricks job runs, standing in for what
`WorkspaceClient().jobs.list_runs(job_id=...)` would return from Databricks'
own run history API.

This is what `demo_databricks.DemoDatabricksWorkspaceComponent`'s observation
sensor reads in demo mode: a run of the job that happened *outside* Dagster
-- a data scientist re-running the notebook manually, or a run kicked off by
another scheduler -- the same way the Databricks Jobs UI's run history would
show it. Dagster observes this history; it never writes it (CLAUDE.md,
"Mock source state lives outside Dagster, representing the upstream
system's own state").

Timestamps are relative to call time, not a fixed calendar date, matching
`adf_legacy_runs.py`'s convention. The most recent run per job lands a few
minutes before call time so it's always inside the sensor's lookback window;
earlier ones extend the history for anyone who winds the cursor back.
"""

from datetime import datetime, timedelta


def list_external_databricks_runs(job_name: str) -> list[dict]:
    anchor = datetime.utcnow() - timedelta(minutes=8)
    return [
        {
            "run_id": f"external-databricks-run-{job_name}-{n}",
            "job_name": job_name,
            "result_state": "SUCCESS",
            "run_start": (anchor - timedelta(days=n, minutes=12)).isoformat(),
            "run_end": (anchor - timedelta(days=n)).isoformat(),
        }
        for n in (2, 1, 0)
    ]
