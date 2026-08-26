"""Alerting on the one asset Kapitus would page someone over: `funded_loans_daily`.

The brief doesn't name a specific alerting channel (Slack/Teams/PagerDuty),
so this uses Dagster's own built-in `run_failure_sensor` -- logged today,
and a one-line swap to a real `dagster-slack` or `dagster-msteams` resource
once Kapitus names their channel. Configured and visible in the UI,
default-stopped so it never fires during an unattended validation run.
"""

import dagster as dg

critical_pipeline_job = dg.define_asset_job(
    name="critical_pipeline_job",
    description="The mart Kapitus would page someone over: funded_loans_daily and everything upstream of it.",
    selection=dg.AssetSelection.assets(dg.AssetKey(["marts", "funded_loans_daily"])).upstream(),
)


@dg.run_failure_sensor(
    monitored_jobs=[critical_pipeline_job],
    default_status=dg.DefaultSensorStatus.STOPPED,
    description="Alerts when a run touching funded_loans_daily or its upstream fails.",
)
def critical_pipeline_failure_alert(context: dg.RunFailureSensorContext) -> None:
    context.log.error(
        "Run %s for job %s failed: %s. Swap this log call for a real dagster-slack or "
        "dagster-msteams post once Kapitus names their alerting channel.",
        context.dagster_run.run_id,
        context.dagster_run.job_name,
        context.failure_event.message,
    )


@dg.definitions
def defs() -> dg.Definitions:
    return dg.Definitions(jobs=[critical_pipeline_job], sensors=[critical_pipeline_failure_alert])
