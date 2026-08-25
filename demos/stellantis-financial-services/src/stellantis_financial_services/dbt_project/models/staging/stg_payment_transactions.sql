-- Typed servicer payment/collections transactions.
select
    payment_id,
    contract_id,
    contract_type,
    cast(payment_date as date) as payment_date,
    amount_paid,
    days_past_due,
    payment_method
from {{ source('raw', 'payment_transactions') }}
where payment_date >= '{{ var("min_date") }}'
  and payment_date <= '{{ var("max_date") }}'
