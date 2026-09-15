-- Typed yellow-taxi trips, filtered to non-null keys and a bounded window.
-- Column names match the public tlc_yellow_trips_2022 schema so switching
-- profiles from DuckDB to BigQuery is a target change, not a rewrite.
select
    trip_id,
    vendor_id,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    passenger_count,
    trip_distance,
    pickup_location_id,
    dropoff_location_id,
    payment_type,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,
    cast(pickup_datetime as date) as pickup_date,
    extract(hour from pickup_datetime) as pickup_hour
from {{ source('raw', 'yellow_trips') }}
where trip_id is not null
  and pickup_datetime is not null
  and dropoff_datetime is not null
  and pickup_location_id is not null
  and dropoff_location_id is not null
  and cast(pickup_datetime as date) >= '{{ var("window_start") }}'
  and cast(pickup_datetime as date) <= '{{ var("window_end") }}'
