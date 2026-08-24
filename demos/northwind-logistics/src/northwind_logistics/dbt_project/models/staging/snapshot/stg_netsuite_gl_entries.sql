select
    gl_entry_id,
    customer_id,
    cast(event_date as date) as event_date,
    invoice_id,
    amount,
    gl_account
from {{ source('raw', 'netsuite_gl_entries') }}
