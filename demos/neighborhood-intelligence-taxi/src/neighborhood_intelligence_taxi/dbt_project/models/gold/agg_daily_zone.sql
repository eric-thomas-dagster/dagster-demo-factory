-- Daily pickup metrics per (pickup_date, pickup_zone) -- the primary
-- neighborhood-intelligence grain: how busy each zone was, day by day.
select
    pickup_date,
    pickup_location_id,
    pickup_zone_name,
    pickup_borough,
    count(*) as trip_count,
    sum(passenger_count) as passenger_count,
    round(sum(trip_distance), 2) as total_distance_miles,
    round(sum(fare_amount), 2) as total_fare_amount,
    round(sum(tip_amount), 2) as total_tip_amount,
    round(sum(total_amount), 2) as total_revenue,
    round(avg(trip_duration_minutes), 2) as avg_trip_duration_minutes
from {{ ref('fct_trips') }}
group by pickup_date, pickup_location_id, pickup_zone_name, pickup_borough
