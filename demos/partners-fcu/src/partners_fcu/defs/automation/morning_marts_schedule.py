"""The one fixed-time trigger in this demo: the morning batch cutoff.

No cutover time is confirmed in the brief -- 06:00 America/New_York is
this build's own assumption of a reasonable overnight credit-union batch
cadence (see README), not a stated SLA.

Native `dg.build_schedule_from_partitioned_job` rather than the community
`cron_schedule` component: that component's partitioned-job mode rejects
`cron_expression`/`execution_timezone` combined with
`partition_type`/`hour_of_day` -- a specific local hour can't be expressed
alongside a `partitions_def`. The native call is one function call, not
worth a component (same registry gap recorded for demos/rvu-tempcover).
"""

import dagster as dg

morning_marts_job = dg.define_asset_job(
    name="partners_fcu_morning_marts_job",
    selection=dg.AssetSelection.assets(
        dg.AssetKey(["marts", "fct_daily_member_activity"]),
        dg.AssetKey(["marts", "fct_loan_portfolio_summary"]),
    ),
)

morning_marts_schedule = dg.build_schedule_from_partitioned_job(
    morning_marts_job,
    hour_of_day=6,
    minute_of_hour=0,
    name="partners_fcu_morning_marts_schedule",
    description=(
        "Computes the prior day's member-activity and loan-portfolio marts at "
        "06:00 America/New_York, once the overnight SQL Server feed is expected "
        "to have landed."
    ),
)
