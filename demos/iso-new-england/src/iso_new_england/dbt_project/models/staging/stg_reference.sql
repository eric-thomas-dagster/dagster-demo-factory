-- Typed operations-advisory events, one row per notice.
select
    notice_id,
    cast(event_date as date) as event_date,
    cast(issued_at as timestamp) as issued_at,
    category,
    severity,
    region,
    message_summary
from {{ source('staged', 'staged_reference') }}
where event_date >= '{{ var("min_date") }}'
  and event_date <= '{{ var("max_date") }}'
