{{ config(materialized='table') }}

select
    offer_id,
    count(*) as total_conversions,
    sum(payout) as total_payout,
    avg(payout) as avg_payout
from {{ ref('stg_affise_conversions') }}
where status = 'approved'
group by offer_id