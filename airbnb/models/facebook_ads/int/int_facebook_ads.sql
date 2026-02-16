{{ config(materialized='incremental', unique_key='ad_id') }}

with f as (
    select *,
           'facebook_ads' as platform,
           'social' as channel
    from {{ ref('stg_facebook_ads') }}
),
fx as (
    select 
        f.*,
        coalesce(f.spend,0)::float * coalesce(c.rate_to_gbp,1) as spend_gbp,
        coalesce(f.revenue,0)::float * coalesce(c.rate_to_gbp,1) as revenue_gbp
    from f
    left join {{ ref('dim_fx_rates') }} c
        on f.date = c.date
        and f.country = c.currency
)

select
    *,
    case when clicks != 0 then spend_gbp / clicks else null end as calc_cpc,
    case when conversions != 0 then spend_gbp / conversions else null end as calc_cpa,
    case when spend_gbp != 0 then revenue_gbp / spend_gbp else null end as calc_roas,
    case when spend_gbp < 0 or revenue_gbp < 0 then 1
         when clicks > impressions then 1
         else 0
    end as data_issue_flag
from fx
