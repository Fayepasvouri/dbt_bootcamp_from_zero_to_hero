{{ config(materialized='view') }}

with source as (
    select *
    from {{ source('marketing_source', 'GOOGLE_ADS_RAW_DATA') }}
),

cleaned as (
    select
        google_ads_id,
        ad_group,
        campaign_name,
        network,
        date::date as date,
        country,
        device,
        coalesce(impressions::int, 0) as impressions,
        coalesce(clicks::int, 0) as clicks,
        coalesce(conversions::int, 0) as conversions,
        coalesce(cost::float, 0) as spend,
        coalesce(revenue::float, 0) as revenue,
        coalesce(cpa::float, 0) as cpa,
        coalesce(cpc::float, 0) as cpc,
        coalesce(ctr_percent::float, 0) as ctr_percent,
        coalesce(roas::float, 0) as roas,

        -- derived metrics for auditing
        case when impressions > 0 then clicks / impressions * 100 else null end as calc_ctr,
        case when clicks > 0 then spend / clicks else null end as calc_cpc,
        case when conversions > 0 then spend / conversions else null end as calc_cpa,
        case when spend > 0 then revenue / spend else null end as calc_roas,

        -- data quality flags
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

        current_timestamp() as record_inserted_at

    from source
)

select * from cleaned
