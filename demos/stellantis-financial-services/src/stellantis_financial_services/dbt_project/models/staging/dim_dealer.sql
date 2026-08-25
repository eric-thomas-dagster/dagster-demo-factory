-- One row per dealer across all three dealer-submitted feeds. Real dealer
-- names/regions were not provided by the brief (flagged unknown) -- this
-- derives a stable, illustrative name and region from the dealer_id itself
-- rather than inventing specific dealership facts.
with dealers as (
    select distinct dealer_id from {{ ref('stg_loan_originations') }}
    union
    select distinct dealer_id from {{ ref('stg_lease_originations') }}
    union
    select distinct dealer_id from {{ source('raw', 'dealer_floorplan_feed') }}
),
numbered as (
    select
        dealer_id,
        cast(regexp_extract(dealer_id, '[0-9]+') as integer) as dealer_number
    from dealers
)
select
    dealer_id,
    dealer_number,
    'Dealer ' || dealer_number as dealer_name,
    case dealer_number % 10
        when 0 then 'MI' when 1 then 'OH' when 2 then 'IN' when 3 then 'IL'
        when 4 then 'WI' when 5 then 'PA' when 6 then 'TX' when 7 then 'FL'
        when 8 then 'CA' else 'GA'
    end as dealer_region
from numbered
