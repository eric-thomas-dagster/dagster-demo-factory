"""History of ADF pipeline runs, standing in for what
`client.pipeline_runs.query_by_factory(...)` would return from ADF's own
run-history API.

This is what the `legacy_orchestration_observation_sensor` reads in demo
mode: runs RVU's own scheduler kicked off, outside Dagster, the same way a
real ADF Monitor tab would show them. Dagster observes this history; it
never writes it (CLAUDE.md, "Mock source state lives outside Dagster,
representing the upstream system's own state").

Timestamps are relative to the moment the sensor actually ticks, not a
fixed calendar date -- the same "arrival timing" pattern CLAUDE.md
describes for late-arriving feeds. The most recent run lands a few minutes
before call time so it's always inside the sensor's default first-tick
lookback window (`datetime.utcnow() - timedelta(hours=1)`, set in the real
`azure_data_factory` component); the two before it extend the history for
anyone who winds the cursor back. Row identity (run_id, pipeline_name,
status) is fixed, so only the offsets from "now" vary between runs, not the
data itself.
"""

from datetime import datetime, timedelta


def list_legacy_adf_runs() -> list[dict]:
    anchor = datetime.utcnow() - timedelta(minutes=5)
    return [
        {
            "run_id": f"legacy-adf-run-{n}",
            "pipeline_name": "legacy_nightly_ingestion",
            "status": "Succeeded",
            "run_start": (anchor - timedelta(days=n, minutes=6)).isoformat(),
            "run_end": (anchor - timedelta(days=n)).isoformat(),
        }
        for n in (2, 1, 0)
    ]
