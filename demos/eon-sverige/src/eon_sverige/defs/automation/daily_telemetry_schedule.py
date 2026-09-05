"""The real deadline the brief implies: grid-load telemetry for a day should
have landed before the 05:00 CET SLA named on its asset metadata. Runs at
03:00 CET (in the daily partitions_def's own Europe/Stockholm timezone), two
hours of slack ahead of it.

Native `dg.build_schedule_from_partitioned_job` rather than the community
`cron_schedule` component, for the same reason recorded on the City of
Detroit DWSD build: that component's partitioned-job mode only accepts
`cron_expression`/`execution_timezone` XOR `hour_of_day`/`minute_of_hour`
(confirmed by reading its `component.py` on that build, 2026-08-28), so a
specific local hour can't be expressed alongside a partitions_def. The
native call is one function call, not worth a component.

Scoped to `raw_grid_load_telemetry` only (not `raw_meter_reads`, which is
multi-partitioned by date x zone) -- `build_schedule_from_partitioned_job`
targets a single partitions_def, and mixing the two into one job would need
a partition mapping this graph-first build has no reason to add.
"""

import dagster as dg

daily_telemetry_ingestion_job = dg.define_asset_job(
    name="eon_daily_telemetry_ingestion_job",
    selection=dg.AssetSelection.assets(dg.AssetKey(["raw_grid_load_telemetry"])),
)

daily_telemetry_ingestion_schedule = dg.build_schedule_from_partitioned_job(
    daily_telemetry_ingestion_job,
    hour_of_day=3,
    minute_of_hour=0,
    name="eon_daily_telemetry_ingestion_schedule",
    description=(
        "Materializes the day's grid-load telemetry two hours ahead of the "
        "05:00 CET SLA on grid_load_hourly's freshness policy."
    ),
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
