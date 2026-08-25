-- Payments that are 30+ days past due -- one row per delinquency event.
-- `fact_delinquency_snapshot` rolls this up to a daily count, and it's the
-- asset SFS's collections team would page someone about (freshness policy).
select
    payment_id,
    contract_id,
    contract_type,
    payment_date,
    amount_paid,
    days_past_due,
    case
        when days_past_due >= 60 then 'severe'
        when days_past_due >= 30 then 'moderate'
    end as delinquency_severity
from {{ ref('stg_payment_transactions') }}
where days_past_due >= 30
