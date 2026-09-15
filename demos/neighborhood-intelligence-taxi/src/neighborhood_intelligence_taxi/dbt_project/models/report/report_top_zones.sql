-- Cross-window ranking of pickup zones by trip volume. Fans out from
-- fct_trips so the report can point at which neighborhoods matter most.
select
    pickup_location_id,
    pickup_zone_name,
    pickup_borough,
    count(*) as trip_count,
    round(sum(fare_amount), 2) as total_fare_amount,
    round(sum(tip_amount), 2) as total_tip_amount,
    round(sum(total_amount), 2) as total_revenue,
    round(avg(trip_distance), 2) as avg_trip_distance_miles,
    row_number() over (order by count(*) desc) as trip_count_rank
from {{ ref('fct_trips') }}
where pickup_zone_name is not null
group by pickup_location_id, pickup_zone_name, pickup_borough
