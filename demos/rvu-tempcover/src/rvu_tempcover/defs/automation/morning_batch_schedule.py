"""The one fixed-time trigger in this demo: the morning batch cutoff.

Quote and policy data from the prior day is expected to have synced via
Fivetran overnight; this schedule computes the daily marts once that
window has closed. No specific cutover time is confirmed in the brief for
this rebuild -- 07:00 Europe/London is this build's own assumption of a
reasonable UK-morning batch cadence (see README), not a stated SLA.

Native `dg.build_schedule_from_partitioned_job` rather than the community
`cron_schedule` component: that component's partitioned-job mode rejects
`cron_expression`/`execution_timezone` combined with
`partition_type`/`hour_of_day` -- a specific local hour can't be expressed
alongside a `partitions_def`. The native call is one function call, not
worth a component.
"""

import dagster as dg

morning_marts_job = dg.define_asset_job(
    name="rvu_morning_marts_job",
    selection=dg.AssetSelection.assets(
        dg.AssetKey(["marts", "fct_quotes_daily"]),
        dg.AssetKey(["marts", "fct_bound_policies_daily"]),
    ),
)

morning_marts_schedule = dg.build_schedule_from_partitioned_job(
    morning_marts_job,
    hour_of_day=7,
    minute_of_hour=0,
    name="rvu_morning_marts_schedule",
    description=(
        "Computes the prior day's quote and bound-policy marts at 07:00 "
        "Europe/London, once the overnight Fivetran sync is expected to "
        "have landed."
    ),
)
