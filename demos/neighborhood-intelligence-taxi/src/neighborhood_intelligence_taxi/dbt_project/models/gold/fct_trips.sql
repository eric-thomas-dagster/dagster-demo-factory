-- Trip fact -- one row per completed yellow-taxi trip, enriched with
-- pickup and dropoff zone metadata for downstream aggregation and reporting.
select
    t.trip_id,
    t.vendor_id,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.pickup_date,
    t.pickup_hour,
    t.passenger_count,
    t.trip_distance,
    t.pickup_location_id,
    pz.zone_name as pickup_zone_name,
    pz.borough as pickup_borough,
    pz.service_zone as pickup_service_zone,
    t.dropoff_location_id,
    dz.zone_name as dropoff_zone_name,
    dz.borough as dropoff_borough,
    dz.service_zone as dropoff_service_zone,
    t.payment_type,
    t.fare_amount,
    t.tip_amount,
    t.tolls_amount,
    t.total_amount,
    -- Trip duration in minutes: derived once here so agg models don't
    -- recompute it (and so the reconciliation check has a stable grain).
    extract(epoch from (t.dropoff_datetime - t.pickup_datetime)) / 60.0
        as trip_duration_minutes
from {{ ref('stg_yellow_trips') }} t
left join {{ ref('stg_taxi_zones') }} pz
    on t.pickup_location_id = pz.zone_id
left join {{ ref('stg_taxi_zones') }} dz
    on t.dropoff_location_id = dz.zone_id
