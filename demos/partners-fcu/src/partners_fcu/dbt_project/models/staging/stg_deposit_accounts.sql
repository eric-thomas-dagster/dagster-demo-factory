-- Typed deposit account snapshot.
select
    account_id,
    member_id,
    account_type,
    balance,
    cast(partition_date as date) as as_of_date
from {{ source('raw', 'raw_deposit_accounts') }}
