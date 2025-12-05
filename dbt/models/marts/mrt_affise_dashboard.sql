{{ config(materialized='table') }}

select
    i.offer_id,
    o.offer_name,
    i.total_conversions,
    i.total_payout,
    i.avg_payout
from {{ ref('int_affise_metrics') }} i
left join {{ ref('dim_offers') }} o
    on i.offer_id = o.offer_id