"""History of ADF pipeline runs, standing in for what
`client.pipeline_runs.query_by_factory(...)` would return from ADF's own
run-history API.

This is what the ADF observation sensor reads in demo mode: runs Stonebranch
kicked off, outside Dagster, the same way a real ADF Monitor tab would show
them. Dagster observes this history; it never writes it.

Timestamps are relative to the moment the sensor actually ticks, not a fixed
calendar date. The most recent run lands a few minutes before call time so
it's always inside the sensor's default first-tick lookback window; the two
before it extend the history for anyone who winds the cursor back.
"""

from datetime import datetime, timedelta


def list_legacy_adf_runs() -> list[dict]:
    anchor = datetime.utcnow() - timedelta(minutes=5)
    return [
        {
            "run_id": f"legacy-adf-run-{n}",
            "pipeline_name": "stonebranch_nightly_mesh_sync",
            "status": "Succeeded",
            "run_start": (anchor - timedelta(days=n, minutes=7)).isoformat(),
            "run_end": (anchor - timedelta(days=n)).isoformat(),
        }
        for n in (2, 1, 0)
    ]
