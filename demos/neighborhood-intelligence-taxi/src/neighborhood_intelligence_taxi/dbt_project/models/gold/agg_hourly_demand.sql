-- Hour-of-day demand curve per pickup borough -- feeds the rush-hour
-- staffing/dispatch talk track.
select
    pickup_borough,
    pickup_hour,
    count(*) as trip_count,
    round(avg(trip_distance), 2) as avg_trip_distance_miles,
    round(avg(fare_amount), 2) as avg_fare_amount,
    round(avg(trip_duration_minutes), 2) as avg_trip_duration_minutes
from {{ ref('fct_trips') }}
where pickup_borough is not null
group by pickup_borough, pickup_hour
