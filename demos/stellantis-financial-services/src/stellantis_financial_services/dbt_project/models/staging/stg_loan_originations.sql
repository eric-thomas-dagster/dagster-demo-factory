-- Typed auto loan origination contracts.
select
    loan_id,
    cast(contract_date as date) as contract_date,
    dealer_id,
    borrower_id,
    vehicle_vin,
    amount_financed,
    apr,
    term_months,
    product_type,
    borrower_state
from {{ source('raw', 'loan_originations') }}
where contract_date >= '{{ var("min_date") }}'
  and contract_date <= '{{ var("max_date") }}'
