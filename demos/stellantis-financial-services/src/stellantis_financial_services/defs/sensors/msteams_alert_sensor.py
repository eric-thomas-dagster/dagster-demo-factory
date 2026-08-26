"""Run-failure alerting to the Teams channel SFS's data team already lives in.

Maps directly to the AE's stated requirement: "Teams/email are how they'd
want notifications." Native `dagster-msteams` integration, not a community
component. Ships stopped by default (zero setup, no webhook to configure) --
turning it on for real is one `dg dev` toggle plus a real `MSTEAMS_WEBHOOK_URL`.
"""

import os

from dagster_msteams import make_teams_on_run_failure_sensor

import dagster as dg

_DEMO_PLACEHOLDER_WEBHOOK = "https://outlook.office.com/webhook/demo-mode-not-configured"


def _failure_message(context: dg.RunFailureSensorContext) -> str:
    return (
        f"SFS demo run failed: job `{context.dagster_run.job_name}` "
        f"(run {context.dagster_run.run_id[:8]}). Check the Dagster+ run page for details."
    )


portfolio_pipeline_failure_sensor = make_teams_on_run_failure_sensor(
    hook_url=os.environ.get("MSTEAMS_WEBHOOK_URL", _DEMO_PLACEHOLDER_WEBHOOK),
    message_fn=_failure_message,
    name="portfolio_pipeline_failure_sensor",
    default_status=dg.DefaultSensorStatus.STOPPED,
)
