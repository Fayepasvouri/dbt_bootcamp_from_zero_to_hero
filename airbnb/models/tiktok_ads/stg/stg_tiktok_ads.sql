{{ config(materialized='view') }}

with source as (
    select *
    from {{ source('marketing_source', 'TIKTOK_ADS_RAW_DATA') }}
),

cleaned as (
    select
        tiktok_ads_id,
        ad_group,
        campaign_name,
        date::date as date,
        country,
        device,
        gender,
        placement,
        coalesce(impressions::int, 0) as impressions,
        coalesce(clicks::int, 0) as clicks,
        coalesce(conversions::int, 0) as conversions,
        coalesce(spend::float, 0) as spend,
        coalesce(revenue::float, 0) as revenue,
        coalesce(video_views::int, 0) as video_views,

        -- derived metrics
        case when impressions > 0 then clicks / impressions * 100 else null end as ctr_percent,
        case when clicks > 0 then spend / clicks else null end as cpc,
        case when conversions > 0 then spend / conversions else null end as cpa,
        case when spend > 0 then revenue / spend else null end as roas,
        case when impressions > 0 then video_views / impressions * 100 else null end as view_rate,

        -- data quality / audit flags
        case 
            when tiktok_ads_id is null then 0
            when impressions < 0 or clicks < 0 or conversions < 0 or spend < 0 or revenue < 0 then 0
            else 1
        end as is_valid,

        case
            when tiktok_ads_id is null then 'missing tiktok_ads_id'
            when impressions < 0 then 'negative impressions'
            when clicks < 0 then 'negative clicks'
            when conversions < 0 then 'negative conversions'
            when spend < 0 then 'negative spend'
            when revenue < 0 then 'negative revenue'
            else null
        end as audit_notes,

        current_timestamp() as record_inserted_at
    from source
)

select * from cleaned
