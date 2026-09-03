-- Daily quote volume and conversion, by channel.
select
    quote_date,
    channel,
    count(*) as quote_count,
    sum(converted_flag) as converted_count,
    round(sum(converted_flag)::double / count(*), 4) as conversion_rate,
    round(sum(premium_quoted), 2) as total_premium_quoted
from {{ ref('stg_quote_requests') }}
group by quote_date, channel
