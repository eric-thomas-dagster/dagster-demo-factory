-- Typed bookings. Dagster's partition bookkeeping for the downstream daily
-- fact is independent of this model's execution grain -- this is a small
-- full-refresh transform over the whole accumulated raw window, not an
-- incremental build scoped to one partition (same pattern used by
-- demos/partners-fcu, demos/rvu-tempcover, demos/kapitus).
select
    booking_id,
    specialist_id,
    consumer_id,
    cast(booking_date as date) as booking_date,
    status,
    price_sek
from {{ source('raw', 'raw_bookings') }}
where booking_id is not null
