-- One row per pickup_date: citywide summary rolling every zone into a
-- single line for the morning-brief report.
select
    pickup_date,
    count(distinct pickup_location_id) as active_zone_count,
    sum(trip_count) as trip_count,
    sum(passenger_count) as passenger_count,
    round(sum(total_distance_miles), 2) as total_distance_miles,
    round(sum(total_fare_amount), 2) as total_fare_amount,
    round(sum(total_tip_amount), 2) as total_tip_amount,
    round(sum(total_revenue), 2) as total_revenue,
    round(sum(total_revenue) / nullif(sum(trip_count), 0), 2) as revenue_per_trip
from {{ ref('agg_daily_zone') }}
group by pickup_date
order by pickup_date
