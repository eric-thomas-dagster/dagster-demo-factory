"""The one fixed-time trigger in this demo.

No cutover time is confirmed in the brief -- 07:00 Europe/Stockholm is
this build's own assumption of a reasonable morning refresh for a
Stockholm-based booking platform (see README), not a stated SLA.

Native `dg.build_schedule_from_partitioned_job` rather than the community
`cron_schedule` component -- that component's partitioned-job mode can't
express a local hour alongside a `partitions_def` (registry gap recorded
for demos/rvu-tempcover and demos/partners-fcu; not re-litigated here).
"""

import dagster as dg

morning_marts_job = dg.define_asset_job(
    name="bokadirekt_morning_marts_job",
    selection=dg.AssetSelection.assets(
        dg.AssetKey(["marts", "fct_bookings_daily"]),
        dg.AssetKey("specialist_utilization_daily"),
    ),
)

morning_marts_schedule = dg.build_schedule_from_partitioned_job(
    morning_marts_job,
    hour_of_day=7,
    minute_of_hour=0,
    name="bokadirekt_morning_marts_schedule",
    description=(
        "Computes the prior day's bookings fact and specialist utilization "
        "rollup at 07:00 Europe/Stockholm."
    ),
)
