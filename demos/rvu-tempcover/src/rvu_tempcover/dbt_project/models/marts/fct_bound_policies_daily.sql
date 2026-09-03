-- Daily bound-policy volume and premium, by panel insurer. Joins the panel
-- feed for the insurer's display name -- a real ref() edge from
-- stg_panel_feed, so this fact genuinely depends on panel data being
-- reconciled, not just on the policy record itself.
with policies as (
    select
        bind_date,
        panel_insurer_id,
        count(*) as policy_count,
        round(sum(premium_amount), 2) as total_premium_amount
    from {{ ref('stg_bound_policies') }}
    group by bind_date, panel_insurer_id
),

latest_panel_status as (
    select
        insurer_id,
        insurer_name,
        row_number() over (partition by insurer_id order by feed_date desc) as rn
    from {{ ref('stg_panel_feed') }}
)

select
    p.bind_date,
    p.panel_insurer_id,
    lp.insurer_name,
    p.policy_count,
    p.total_premium_amount
from policies p
left join latest_panel_status lp
    on lp.insurer_id = p.panel_insurer_id and lp.rn = 1
