-- Typed panel insurer capacity feed.
select
    insurer_id,
    insurer_name,
    cast(feed_date as date) as feed_date,
    panel_status,
    capacity_remaining_pct
from {{ source('raw', 'panel_insurer_feed') }}
where feed_date >= '{{ var("min_date") }}'
  and feed_date <= '{{ var("max_date") }}'
