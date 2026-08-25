-- Daily delinquency rollup -- the collections team's paged-on-freshness asset.
select
    payment_date as snapshot_date,
    count(*) as delinquency_event_count,
    sum(case when delinquency_severity = 'severe' then 1 else 0 end) as severe_count,
    sum(amount_paid) as amount_collected_on_delinquent_accounts
from {{ ref('stg_delinquency_events') }}
group by payment_date
