{{
    config(
        unique_key=['rate_date', 'carrier', 'lane_code'],
    )
}}

-- Validated freight rate quotes. This model itself never fails when a
-- carrier's data is missing for a day -- that is legitimate (if incomplete)
-- data. `tests/carrier_rate_arrival.sql` is what turns "regional_ltl_b is
-- missing for this partition" into a loud, blocking failure that stops
-- margin_by_lane_customer from computing a wrong number.
select
    carrier,
    rate_date,
    lane_code,
    rate_amount_usd,
    fuel_surcharge_pct,
    transit_days
from {{ source('raw', 'carrier_rate_raw') }}
where rate_date >= '{{ var("min_date") }}'
  and rate_date < '{{ var("max_date") }}'
