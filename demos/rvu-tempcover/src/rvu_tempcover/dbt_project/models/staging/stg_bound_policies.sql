-- Typed bound policies.
select
    policy_id,
    cast(bind_date as date) as bind_date,
    quote_id,
    customer_id,
    policy_type,
    premium_amount,
    panel_insurer_id,
    nullif(partner_id, '') as partner_id
from {{ source('raw', 'bound_policies') }}
where bind_date >= '{{ var("min_date") }}'
  and bind_date <= '{{ var("max_date") }}'
