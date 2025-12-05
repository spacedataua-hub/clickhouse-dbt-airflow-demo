{{ config(materialized='view') }}

select
    offer_id,
    affiliate_id,
    cast(conversion_time as timestamp) as conversion_time,
    cast(payout as decimal(18,2)) as payout,
    lower(status) as status
from {{ source('clickhouse', 'affise_conversions_raw') }}
where conversion_time is not null
group by
    offer_id,
    affiliate_id,
    conversion_time,
    payout,
    status