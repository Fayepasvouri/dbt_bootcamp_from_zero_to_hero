{{ config(materialized='view') }}

with source as (
    select *
    from {{ source('marketing_source', 'FACEBOOK_ADS_RAW_DATA') }}
),

cleaned as (
    select
        ad_id,
        campaign_name,
        adset_name,
        ad_name,
        date::date as date,
        country,
        device,
        impressions::int as impressions,
        clicks::int as clicks,
        conversions::int as conversions,
        spend::float as spend,
        revenue::float as revenue,

        -- derived metrics
        case when impressions != 0 then clicks / impressions * 100 else null end as ctr_percent,
        case when clicks != 0 then spend / clicks else null end as cpc,
        case when conversions != 0 then spend / conversions else null end as cpa,
        case when spend != 0 then revenue / spend else null end as roas
    from source
)

select * from cleaned
