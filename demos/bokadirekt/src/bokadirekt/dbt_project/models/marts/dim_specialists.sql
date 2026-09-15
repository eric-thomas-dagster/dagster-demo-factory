-- One row per specialist, with lifetime completed-booking volume --
-- a real ref() edge onto both staging models.
select
    s.specialist_id,
    s.specialist_name,
    s.region,
    s.service_category,
    s.onboarded_date,
    count(b.booking_id) filter (where b.status = 'completed') as lifetime_completed_bookings
from {{ ref('stg_specialists') }} as s
left join {{ ref('stg_bookings') }} as b
    on s.specialist_id = b.specialist_id
group by 1, 2, 3, 4, 5
