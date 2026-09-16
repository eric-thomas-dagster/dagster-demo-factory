"""Monthly schedule -- native Dagster equivalent of the parent's
`cron_schedule` component.

`build_schedule_from_partitioned_job` derives the cron cadence from the
job's `MonthlyPartitionsDefinition`, so the schedule fires once per
partition boundary. Default status is STOPPED so a fresh clone doesn't
kick off runs the prospect didn't ask for.
"""

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    build_schedule_from_partitioned_job,
    define_asset_job,
)

from neighborhood_intelligence_taxi_python.partitions import MONTHLY_PARTITIONS

monthly_full_pipeline_job = define_asset_job(
    name="monthly_full_pipeline_job",
    selection=AssetSelection.groups("silver", "gold", "report"),
    partitions_def=MONTHLY_PARTITIONS,
)

monthly_full_pipeline_schedule = build_schedule_from_partitioned_job(
    job=monthly_full_pipeline_job,
    default_status=DefaultScheduleStatus.STOPPED,
)
