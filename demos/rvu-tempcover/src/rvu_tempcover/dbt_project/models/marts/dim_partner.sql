-- One row per corporate partner / broker on Tempcover's panel.
select
    partner_id,
    partner_name,
    partner_type,
    commission_rate,
    active_flag
from {{ source('raw', 'partner_broker_feed') }}
