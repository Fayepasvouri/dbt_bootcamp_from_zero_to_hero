{{ config(materialized='view') }}

with source as (
    select *
    from {{ source('marketing_source', 'FACEBOOK_ADS_RAW_DATA') }}
),

fx_rates as (
    select 
        date,
        currency,
        rate_to_gbp
    from {{ ref('dim_fx_rates') }}
    where currency = 'USD'
),

cleaned as (
    select
        s.ad_id,
        s.campaign_name,
        s.adset_name,
        s.ad_name,
        s.date::date as date,
        s.country,
        s.device,
        s.impressions::int as impressions,
        s.clicks::int as clicks,
        s.conversions::int as conversions,
        {{ safe_divide('s.spend', 'fx.rate_to_gbp', 's.spend') }}::float as spend_gbp,
        {{ safe_divide('s.revenue', 'fx.rate_to_gbp', 's.revenue') }}::float as revenue_gbp,
        s.spend::float as spend,
        s.revenue::float as revenue,

        -- derived metrics using safe divide
        {{ safe_divide('s.clicks', 's.impressions', 0) }} * 100 as ctr_percent,
        {{ safe_divide('s.spend', 's.clicks', 0) }} as cpc,
        {{ safe_divide('s.spend', 's.conversions', 0) }} as cpa,
        {{ safe_divide('s.revenue', 's.spend', 0) }} as roas
    from source s
    left join fx_rates fx on s.date::date = fx.date and fx.currency = 'USD'
)

select * from cleaned
