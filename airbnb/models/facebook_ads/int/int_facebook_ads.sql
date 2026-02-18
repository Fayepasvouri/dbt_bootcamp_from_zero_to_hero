-- depends_on: {{ ref('stg_facebook_ads') }}

with base as (
    select
        ad_id,
        campaign_name,
        adset_name,
        ad_name,
        date,
        country,
        device,
        impressions,
        clicks,
        conversions,
        spend,
        spend_gbp,
        revenue,
        revenue_gbp,
        ctr_percent,
        cpc,
        cpa,
        roas
    from {{ ref('stg_facebook_ads') }}
)

select * from base
