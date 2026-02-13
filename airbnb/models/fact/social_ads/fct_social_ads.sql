{{ config(materialized='table') }}

with facebook as (
    select
        ad_id as platform_id,
        'facebook' as platform,
        campaign_name,
        adset_name,
        date,
        country,
        device,
        impressions, 
        clicks, 
        conversions, 
        spend, 
        revenue, 
        ctr_percent, cpc, cpa, roas
    from {{ ref('int_facebook_ads') }}
),
tiktok as (
    select
        ad_id as platform_id,
        'tiktok' as platform,
        campaign_name,
        ad_group as adset_name,
        date,
        country,
        device,
        impressions, 
        clicks, 
        conversions, 
        spend, 
        revenue, 
        calc_ctr as ctr_percent,
        calc_cpc as cpc, 
        calc_cpa as cpa,
        calc_roas as roas
    from {{ ref('int_tiktok_ads') }}
)

select *
from facebook
union all
select *
from tiktok
