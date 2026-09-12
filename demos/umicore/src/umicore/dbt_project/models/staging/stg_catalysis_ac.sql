-- Typed Automotive Catalysts (AC) daily feed.
select
    feed_record_id,
    catalyst_unit_id,
    conversion_rate_pct,
    throughput_units,
    cast(recorded_on as date) as feed_date
from {{ source('raw', 'ac_daily_feed') }}
