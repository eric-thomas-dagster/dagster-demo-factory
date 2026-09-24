"""The real deadline this build assumes (not named in the brief): a US
equity index needs its constituent list settled before market open (9:30 AM
ET). This runs at 5:00 AM ET, giving the price/corporate-action ingest,
eager-automated `barrons_400_constituents` recompute, and downstream
egress/reporting chain time to complete well ahead of it. This build's own
assumption -- flagged in the README/notification, same as the SSIS
freshness-lag threshold.

Native `dg.build_schedule_from_partitioned_job` rather than the community
`cron_schedule` component -- same registry gap already recorded for
demos/detroit-dwsd (`component-feedback/2026-08-28-graph-first-assets.md`):
that component's partitioned-job mode can't express a specific local hour
alongside a partitions_def. One function call, not worth a component.
"""

import dagster as dg

daily_ingestion_job = dg.define_asset_job(
    name="marketgrader_daily_ingestion_job",
    selection=dg.AssetSelection.assets(
        dg.AssetKey(["raw_price_feed"]),
        dg.AssetKey(["raw_corporate_actions"]),
    ),
)

daily_ingestion_schedule = dg.build_schedule_from_partitioned_job(
    daily_ingestion_job,
    hour_of_day=5,
    minute_of_hour=0,
    name="marketgrader_daily_ingestion_schedule",
    description=(
        "Materializes the day's price + corporate-action feeds 4.5 hours ahead of "
        "the 9:30 AM ET market open -- barrons_400_constituents and the egress/"
        "reporting chain recompute automatically behind it via eager automation."
    ),
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
