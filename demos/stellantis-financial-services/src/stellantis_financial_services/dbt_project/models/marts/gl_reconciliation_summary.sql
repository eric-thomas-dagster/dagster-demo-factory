-- Daily tie-out between servicer collections and the GL posting. Real GL
-- access was not in scope for this demo (illustrative posting, not sourced)
-- -- this shows the capability (a reconciliation surfaced as an asset,
-- not a spreadsheet) rather than a real variance investigation.
select
    payment_date as gl_date,
    count(*) as transaction_count,
    sum(amount_paid) as total_collected,
    sum(amount_paid) as gl_posted_amount,
    0.0 as variance_amount
from {{ ref('stg_payment_transactions') }}
group by payment_date
