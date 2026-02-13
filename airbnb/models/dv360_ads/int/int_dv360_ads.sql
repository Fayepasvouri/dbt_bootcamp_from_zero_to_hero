{{ config(materialized='table') }}

with cleaned as (
    select
        DV360_ID as ad_id,
        campaign_name,
        line_item as adset_name,
        creative_name as ad_name,
        date::date as date,
        country,
        device,
        coalesce(impressions,0)::int as impressions,
        coalesce(clicks,0)::int as clicks,
        coalesce(conversions,0)::int as conversions,
        coalesce(spend,0)::float as spend,
        coalesce(revenue,0)::float as revenue,
        coalesce(video_completions,0)::int as video_views,
        case when impressions != 0 then clicks / impressions * 100 else null end as ctr_percent,
        case when clicks != 0 then spend / clicks else null end as cpc,
        case when conversions != 0 then spend / conversions else null end as cpa,
        case when spend != 0 then revenue / spend else null end as roas,
        case when spend < 0 or revenue < 0 then 1
             when clicks > impressions then 1
             else 0
        end as data_issue_flag,
        'dv360' as platform,
        'paid_media' as channel
    from {{ ref('stg_dv360_ads') }}
)

select * from cleaned
