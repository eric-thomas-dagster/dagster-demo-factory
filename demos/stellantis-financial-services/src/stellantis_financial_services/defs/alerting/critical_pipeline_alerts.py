"""Teams alerting on the two assets SFS would page someone over.

Native `dagster-msteams`, per the brief's explicit ask ("Teams/email are how
they'd want notifications"). `make_teams_on_run_failure_sensor` builds its
own `TeamsClient` at fire time, not at definition time, and defaults to
`DefaultSensorStatus.STOPPED` -- so it is configured and visible in the UI,
pointing at a real (if placeholder, in demo mode) webhook, without ever
making a network call during validation or an unattended demo run. Flipping
it on is an operator action in the UI, and posting to SFS's real Teams
channel is a one-line `SFS_TEAMS_WEBHOOK_URL` change, not a code change.
"""

import os

import dagster as dg
from dagster_msteams import make_teams_on_run_failure_sensor

critical_pipeline_job = dg.define_asset_job(
    name="critical_pipeline_job",
    description="The two assets SFS would page someone over: ABS pool eligibility and the delinquency snapshot.",
    selection=dg.AssetSelection.assets(
        dg.AssetKey(["marts", "abs_pool_eligibility"]),
        dg.AssetKey(["marts", "fact_delinquency_snapshot"]),
    ).upstream(),
)

critical_pipeline_teams_alert = make_teams_on_run_failure_sensor(
    name="critical_pipeline_teams_alert",
    hook_url=os.environ.get(
        "SFS_TEAMS_WEBHOOK_URL", "https://demo-mode-no-webhook-configured.example/webhook"
    ),
    monitored_jobs=[critical_pipeline_job],
    webserver_base_url=os.environ.get("SFS_WEBSERVER_BASE_URL"),
)


@dg.definitions
def defs() -> dg.Definitions:
    return dg.Definitions(jobs=[critical_pipeline_job], sensors=[critical_pipeline_teams_alert])
