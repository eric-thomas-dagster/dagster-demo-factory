-- Typed loan originations.
select
    loan_id,
    member_id,
    loan_type,
    principal_amount,
    cast(partition_date as date) as origination_date
from {{ source('raw', 'raw_loan_originations') }}
