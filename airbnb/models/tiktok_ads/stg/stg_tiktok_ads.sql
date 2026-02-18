{{ config(materialized='view') }}

with source as (
    select *
    from {{ source('marketing_source', 'TIKTOK_ADS_RAW_DATA') }}
),

fx_rates as (
    select 
        date,
        currency,
        rate_to_gbp
    from {{ ref('dim_fx_rates') }}
    where currency = 'CNY'
),

cleaned as (
    select
        s.tiktok_ads_id,
        s.ad_group,
        s.campaign_name,
        s.date::date as date,
        s.country,
        s.device,
        s.gender,
        s.placement,
        coalesce(s.impressions::int, 0) as impressions,
        coalesce(s.clicks::int, 0) as clicks,
        coalesce(s.conversions::int, 0) as conversions,
        coalesce({{ safe_divide('s.spend', 'fx.rate_to_gbp', 's.spend') }}, 0)::float as spend_gbp,
        coalesce({{ safe_divide('s.revenue', 'fx.rate_to_gbp', 's.revenue') }}, 0)::float as revenue_gbp,
        coalesce(s.spend::float, 0) as spend,
        coalesce(s.revenue::float, 0) as revenue,
        coalesce(s.video_views::int, 0) as video_views,

        -- derived metrics using safe divide
        {{ safe_divide('s.clicks', 's.impressions', 0) }} * 100 as ctr_percent,
        {{ safe_divide('s.spend', 's.clicks', 0) }} as cpc,
        {{ safe_divide('s.spend', 's.conversions', 0) }} as cpa,
        {{ safe_divide('s.revenue', 's.spend', 0) }} as roas,
        {{ safe_divide('s.video_views', 's.impressions', 0) }} * 100 as view_rate,

        -- data quality / audit flags
        case 
            when s.tiktok_ads_id is null then 0
            when s.impressions < 0 or s.clicks < 0 or s.conversions < 0 or s.spend < 0 or s.revenue < 0 then 0
            else 1
        end as is_valid,

        case
            when s.tiktok_ads_id is null then 'missing tiktok_ads_id'
            when s.impressions < 0 then 'negative impressions'
            when s.clicks < 0 then 'negative clicks'
            when s.conversions < 0 then 'negative conversions'
            when s.spend < 0 then 'negative spend'
            when s.revenue < 0 then 'negative revenue'
            else null
        end as audit_notes,

        current_timestamp() as record_inserted_at
    from source s
    left join fx_rates fx on s.date::date = fx.date and fx.currency = 'CNY'
)

select * from cleaned
