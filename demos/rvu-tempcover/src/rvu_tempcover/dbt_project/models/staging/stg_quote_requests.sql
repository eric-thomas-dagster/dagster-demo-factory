-- Typed quote requests.
select
    quote_id,
    cast(quote_date as date) as quote_date,
    customer_id,
    channel,
    policy_type,
    premium_quoted,
    converted_flag
from {{ source('raw', 'quote_requests') }}
where quote_date >= '{{ var("min_date") }}'
  and quote_date <= '{{ var("max_date") }}'
