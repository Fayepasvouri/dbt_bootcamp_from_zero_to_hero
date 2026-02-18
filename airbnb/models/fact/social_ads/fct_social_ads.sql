{{ config(materialized='table') }}

-- Column mapping configuration for dynamic union
{% set platforms = {
    'facebook': {
        'model': 'int_facebook_ads',
        'platform': 'facebook',
        'id_column': 'platform_id',
        'adset_column': 'adset_name'
    },
    'tiktok': {
        'model': 'int_tiktok_ads',
        'platform': 'tiktok',
        'id_column': 'ad_id',
        'adset_column': 'ad_group',
        'metric_prefix': 'calc_'
    }
} %}

with facebook as (
    select
        ad_id as platform_id,
        {{ facebook_platform_metadata() }},
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
        'social' as channel,
        'social' as platform_type,
        true as is_programmatic,
        'CNY' as currency,
        'CN' as default_country,
        'tiktok_ads' as platform_name,
        current_timestamp() as metadata_loaded_at,
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
        ctr_percent,
        cpc, 
        cpa,
        roas
    from {{ ref('int_tiktok_ads') }}
)

select *
from facebook
union all
select *
from tiktok
