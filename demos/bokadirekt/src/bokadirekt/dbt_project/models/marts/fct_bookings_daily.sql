-- Daily booking volume and revenue -- the asset a Bokadirekt data person
-- would page someone about (freshness policy is set on this asset in
-- defs/transformation/marts/defs.yaml).
select
    booking_date,
    count(*) as booking_count,
    count(*) filter (where status = 'completed') as completed_count,
    count(*) filter (where status = 'cancelled') as cancelled_count,
    count(*) filter (where status = 'no_show') as no_show_count,
    count(distinct specialist_id) as active_specialist_count,
    round(sum(price_sek) filter (where status = 'completed'), 2) as completed_revenue_sek
from {{ ref('stg_bookings') }}
group by booking_date
