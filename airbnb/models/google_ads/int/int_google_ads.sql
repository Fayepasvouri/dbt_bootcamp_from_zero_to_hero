{{ config(materialized='view') }}

with cleaned as (
    select
        google_ads_id as ad_id,
        ad_group,
        campaign_name,
        network,
        date::date as date,
        country,
        device,
        coalesce(impressions,0)::int as impressions,
        coalesce(clicks,0)::int as clicks,
        coalesce(conversions,0)::int as conversions,
        coalesce(spend,0)::float as spend,
        coalesce(revenue,0)::float as revenue,
        coalesce(cpa,0)::float as cpa,
        coalesce(cpc,0)::float as cpc,
        coalesce(ctr_percent,0)::float as ctr_percent,
        coalesce(roas,0)::float as roas,
        
        -- Recalculate metrics to ensure integrity
        case when impressions > 0 then clicks / impressions * 100 else null end as calc_ctr,
        case when clicks > 0 then spend / clicks else null end as calc_cpc,
        case when conversions > 0 then spend / conversions else null end as calc_cpa,
        case when spend > 0 then revenue / spend else null end as calc_roas,

        -- Data quality checks
        case 
            when google_ads_id is null then 0
            when impressions < 0 or clicks < 0 or conversions < 0 or spend < 0 or revenue < 0 then 0
            else 1
        end as is_valid,

        case
            when google_ads_id is null then 'missing google_ads_id'
            when impressions < 0 then 'negative impressions'
            when clicks < 0 then 'negative clicks'
            when conversions < 0 then 'negative conversions'
            when spend < 0 then 'negative spend'
            when revenue < 0 then 'negative revenue'
            else null
        end as audit_notes,

        current_timestamp() as record_inserted_at,
        'google' as platform,
        'paid_media' as channel

    from {{ ref('stg_google_ads') }}
)

select * from cleaned
