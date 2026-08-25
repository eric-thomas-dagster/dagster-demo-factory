-- Typed lease origination contracts.
select
    lease_id,
    cast(contract_date as date) as contract_date,
    dealer_id,
    borrower_id,
    vehicle_vin,
    capitalized_cost,
    residual_value,
    monthly_payment,
    term_months,
    product_type,
    borrower_state
from {{ source('raw', 'lease_originations') }}
where contract_date >= '{{ var("min_date") }}'
  and contract_date <= '{{ var("max_date") }}'
