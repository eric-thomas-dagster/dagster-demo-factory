select
    ticket_id,
    customer_id,
    cast(event_date as date) as event_date,
    subject,
    status,
    priority
from {{ source('raw', 'zendesk_tickets') }}
