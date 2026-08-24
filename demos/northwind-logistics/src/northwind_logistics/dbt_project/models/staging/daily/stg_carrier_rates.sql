{{ config(unique_key='event_date') }}
-- Carries the blocking `carrier_rate_arrival` asset check (see
-- `defs/checks/carrier_rate_arrival.py`): if a carrier's rate feed didn't
-- arrive for a day, this model simply has fewer rows for that day than
-- expected -- there is nothing here to "fail" on, which is the point. The
-- check is what turns "fewer rows than expected" into a loud, blocking
-- signal instead of a quietly-wrong margin number downstream.
select
    carrier,
    cast(event_date as date) as event_date,
    lane,
    rate_per_mile,
    fuel_surcharge_pct,
    cast(quoted_at as timestamp) as quoted_at
from {{ source('raw', 'carrier_rate_raw') }}
where event_date >= '{{ var("min_date") }}'
  and event_date <= '{{ var("max_date") }}'
