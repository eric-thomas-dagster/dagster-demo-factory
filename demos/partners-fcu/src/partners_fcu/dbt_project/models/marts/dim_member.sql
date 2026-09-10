-- One row per member seen across transactions, deposits, or loans --
-- a real ref() edge onto all three staging models, so this dimension
-- genuinely depends on the whole ingestion layer being reconciled.
with member_ids as (
    select member_id from {{ ref('stg_member_transactions') }}
    union
    select member_id from {{ ref('stg_deposit_accounts') }}
    union
    select member_id from {{ ref('stg_loan_originations') }}
)

select distinct member_id
from member_ids
