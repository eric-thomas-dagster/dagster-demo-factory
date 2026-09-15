-- Per-vendor performance summary -- CMT (1) vs VeriFone (2), the two TLC
-- meter vendors. Tip and revenue economics side by side.
select
    vendor_id,
    count(*) as trip_count,
    sum(passenger_count) as passenger_count,
    round(sum(fare_amount), 2) as total_fare_amount,
    round(sum(tip_amount), 2) as total_tip_amount,
    round(sum(total_amount), 2) as total_revenue,
    round(avg(trip_distance), 2) as avg_trip_distance_miles,
    round(avg(trip_duration_minutes), 2) as avg_trip_duration_minutes,
    round(sum(tip_amount) / nullif(sum(fare_amount), 0), 4) as tip_to_fare_ratio
from {{ ref('fct_trips') }}
group by vendor_id
