{{ config(
    materialized='table'
) }}

with google_ads as (
    select
        ad_id,
        'google_ads' as platform,
        campaign_name,
        ad_group,
        null as ad_name,
        date::date as date,
        country,
        device,
        coalesce(impressions,0)::int as impressions,
        coalesce(clicks,0)::int as clicks,
        coalesce(conversions,0)::int as conversions,
        coalesce(spend,0)::float as spend,
        coalesce(revenue,0)::float as revenue,
        coalesce(ctr_percent,0)::float as ctr_percent,
        coalesce(cpc,0)::float as cpc,
        coalesce(cpa,0)::float as cpa,
        coalesce(roas,0)::float as roas,
        coalesce(is_valid,1)::int as is_valid,
        audit_notes,
        record_inserted_at
    from {{ ref('int_google_ads') }}
),

dv360 as (
    select
        ad_id,
        'dv360' as platform,
        campaign_name,
        adset_name as ad_group,
        ad_name,
        date::date as date,
        country,
        device,
        coalesce(impressions,0)::int as impressions,
        coalesce(clicks,0)::int as clicks,
        coalesce(conversions,0)::int as conversions,
        coalesce(spend,0)::float as spend,
        coalesce(revenue,0)::float as revenue,
        coalesce(ctr_percent,0)::float as ctr_percent,
        coalesce(cpc,0)::float as cpc,
        coalesce(cpa,0)::float as cpa,
        coalesce(roas,0)::float as roas,
        null as is_valid,
        null as audit_notes,
        null as record_inserted_at
    from {{ ref('int_dv360_ads') }}
)

select * from google_ads
union all
select * from dv360