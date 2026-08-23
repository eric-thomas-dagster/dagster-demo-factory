{{
    config(
        unique_key=['cost_date', 'carrier'],
        meta={
            'dagster': {
                'group': 'transformation',
                'auto_materialize_policy': {'type': 'eager'},
            }
        },
    )
}}

select
    rate_date as cost_date,
    carrier,
    count(distinct lane_code) as lanes_active,
    avg(rate_amount_usd) as avg_rate_amount_usd,
    avg(fuel_surcharge_pct) as avg_fuel_surcharge_pct,
    avg(transit_days) as avg_transit_days
from {{ ref('carrier_rate_validated') }}
where rate_date >= '{{ var("min_date") }}'
  and rate_date < '{{ var("max_date") }}'
group by 1, 2
