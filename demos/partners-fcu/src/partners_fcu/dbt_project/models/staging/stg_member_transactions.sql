-- Typed member transactions.
select
    transaction_id,
    member_id,
    transaction_type,
    amount,
    cast(partition_date as date) as transaction_date
from {{ source('raw', 'raw_member_transactions') }}
