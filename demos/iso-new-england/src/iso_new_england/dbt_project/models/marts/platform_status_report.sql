-- The money-shot asset: a per-day platform status view -- Andrew's "communication
-- mechanism for users around what is happening with the data platform," almost
-- verbatim. Carries a freshness policy (defs/transformation/marts/defs.yaml) so a
-- stale status is itself visible, and an eager automation condition so it never
-- waits on a manual click.
with summary as (
    select
        *,
        round(1.0 * estimated_interval_count / nullif(total_intervals, 0), 4) as estimated_data_rate
    from {{ ref('mart_daily_operations_summary') }}
)

select
    event_date,
    reporting_point_count,
    total_intervals,
    avg_reading_mw,
    peak_reading_mw,
    advisory_count,
    estimated_data_rate,
    case
        when advisory_count > 0 then 'advisory_active'
        -- a handful of estimated readings is routine sensor noise; only a
        -- meaningfully elevated rate is worth flagging
        when estimated_data_rate > 0.05 then 'degraded_quality'
        else 'nominal'
    end as platform_status
from summary
order by event_date
