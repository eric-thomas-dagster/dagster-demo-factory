"""Nightly invoice billing schedule.

Finance needs `invoice_billing_nightly` materialized before the 6am ET close.
`carrier_cost_allocation` and `margin_by_lane_customer` recompute on their own
via AutomationCondition.eager() -- this is the one asset in the graph that
runs on a fixed clock instead, because finance's deadline is a fixed clock,
not a data-dependency.
"""

import dagster as dg

# Must match the start_date used everywhere else this project defines a daily
# partitions definition (ingestion components, dbt template_vars.py).
_START_DATE = "2026-08-17"

invoice_billing_nightly_job = dg.define_asset_job(
    name="invoice_billing_nightly_job",
    selection=dg.AssetSelection.assets("invoice_billing_nightly"),
)


@dg.schedule(
    job=invoice_billing_nightly_job,
    cron_schedule="0 6 * * *",
    execution_timezone="America/New_York",
)
def invoice_billing_nightly_schedule(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
    partitions_def = dg.DailyPartitionsDefinition(start_date=_START_DATE)
    partition_key = partitions_def.get_last_partition_key(
        current_time=context.scheduled_execution_time
    )
    return dg.RunRequest(partition_key=partition_key)


@dg.definitions
def schedule_defs() -> dg.Definitions:
    return dg.Definitions(
        jobs=[invoice_billing_nightly_job],
        schedules=[invoice_billing_nightly_schedule],
    )
