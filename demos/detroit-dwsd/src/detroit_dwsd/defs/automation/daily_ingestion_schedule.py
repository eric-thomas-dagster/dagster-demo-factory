"""The real deadline the brief implies: meter reads and lab readings for a
day should have landed before the 6:00 AM ET SLA named on their asset
metadata. Runs at 4:00 (in the partitions_def's own America/New_York
timezone), two hours of slack ahead of it.

Native `dg.build_schedule_from_partitioned_job` rather than the community
`cron_schedule` component: that component's partitioned-job mode only
accepts `cron_expression`/`execution_timezone` XOR
`hour_of_day`/`minute_of_hour` (confirmed by reading its `component.py` --
both call shapes raised `CheckError`/`Invariant failed` when combined with
`partition_type`), so a specific local hour can't be expressed alongside a
partitions_def. The native call is one function call, not worth a component.
"""

import dagster as dg

daily_ingestion_job = dg.define_asset_job(
    name="dwsd_daily_ingestion_job",
    selection=dg.AssetSelection.assets(
        dg.AssetKey(["meter_reading_extract"]),
        dg.AssetKey(["water_quality_lab_extract"]),
    ),
)

daily_ingestion_schedule = dg.build_schedule_from_partitioned_job(
    daily_ingestion_job,
    hour_of_day=4,
    minute_of_hour=0,
    name="dwsd_daily_ingestion_schedule",
    description=(
        "Materializes the day's meter-read and water-quality-lab extracts two hours "
        "ahead of the 6:00 AM ET SLA on their downstream warehouse assets."
    ),
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
