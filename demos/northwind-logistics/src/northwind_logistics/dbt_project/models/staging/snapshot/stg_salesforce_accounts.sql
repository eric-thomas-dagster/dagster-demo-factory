select
    customer_id,
    customer_name,
    segment,
    region,
    cast(created_at as date) as account_created_date
from {{ source('raw', 'salesforce_accounts') }}
