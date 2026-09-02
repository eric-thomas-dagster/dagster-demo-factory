"""The real deadline this demo implies: end-of-day position and exposure
figures should be computed once the day's trading activity is captured.
Trafigura trades across Singapore, Geneva, Houston, Montevideo, and Mumbai
with no single confirmed close time, so this schedule targets 22:00 UTC --
after all major market sessions in that spread have closed for the day --
rather than assume any one desk's local convention. This hour is this
build's own assumption, not a confirmed SLA (see README).

Native `dg.build_schedule_from_partitioned_job` rather than the community
`cron_schedule` component: that component's partitioned-job mode only
accepts `cron_expression`/`execution_timezone` XOR
`hour_of_day`/`minute_of_hour` (confirmed by reading its `component.py` in
the detroit-dwsd build -- both call shapes raised `CheckError`/`Invariant
failed` when combined with `partition_type`), so a specific hour can't be
expressed alongside a `partitions_def`. The native call is one function
call, not worth a component.
"""

import dagster as dg

eod_risk_job = dg.define_asset_job(
    name="trafigura_eod_risk_job",
    selection=dg.AssetSelection.assets(
        dg.AssetKey(["fact_trade_position_daily"]),
        dg.AssetKey(["fact_credit_exposure_daily"]),
    ),
)

eod_risk_schedule = dg.build_schedule_from_partitioned_job(
    eod_risk_job,
    hour_of_day=22,
    minute_of_hour=0,
    name="trafigura_eod_risk_schedule",
    description=(
        "Computes the day's trade position and credit exposure partitions at "
        "22:00 UTC, after all major trading sessions have closed for the day."
    ),
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
