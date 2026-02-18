{{ config(materialized='view') }}

with source as (
    select *
    from {{ source('marketing_source', 'DV360_RAW_DATA') }}
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
        s.dv360_id,
        s.advertiser,
        s.campaign_name,
        s.insertion_order,
        s.line_item,
        s.creative_name,
        s.date::date as date,
        s.country,
        s.device,
        s.exchange,
        coalesce(s.impressions::int, 0) as impressions,
        coalesce(s.clicks::int, 0) as clicks,
        coalesce(s.conversions::int, 0) as conversions,
        coalesce({{ safe_divide('s.cost', 'fx.rate_to_gbp', 's.cost') }}, 0)::float as spend_gbp,
        coalesce({{ safe_divide('s.revenue', 'fx.rate_to_gbp', 's.revenue') }}, 0)::float as revenue_gbp,
        coalesce(s.cost::float, 0) as spend,
        coalesce(s.revenue::float, 0) as revenue,
        coalesce(s.video_completions::int, 0) as video_completions,
        coalesce(s.viewable_impressions::int, 0) as viewable_impressions,
        coalesce(s.cpa::float, 0) as cpa,
        coalesce(s.cpc::float, 0) as cpc,
        coalesce(s.cpm::float, 0) as cpm,
        coalesce(s.ctr_percent::float, 0) as ctr_percent,
        coalesce(s.roas::float, 0) as roas,
        coalesce(s.viewability_rate_percent::float, 0) as viewability_rate_percent,

        -- derived metrics using safe divide
        {{ safe_divide('s.clicks', 's.impressions', 0) }} * 100 as calc_ctr,
        {{ safe_divide('s.cost', 's.clicks', 0) }} as calc_cpc,
        {{ safe_divide('s.cost', 's.conversions', 0) }} as calc_cpa,
        {{ safe_divide('s.revenue', 's.cost', 0) }} as calc_roas,
        {{ safe_divide('s.viewable_impressions', 's.impressions', 0) }} * 100 as calc_viewability,

        -- data quality flags
        case 
            when s.dv360_id is null then 0
            when s.impressions < 0 or s.clicks < 0 or s.conversions < 0 or s.cost < 0 or s.revenue < 0 then 0
            else 1
        end as is_valid,

        case
            when s.dv360_id is null then 'missing dv360_id'
            when s.impressions < 0 then 'negative impressions'
            when s.clicks < 0 then 'negative clicks'
            when s.conversions < 0 then 'negative conversions'
            when s.cost < 0 then 'negative spend'
            when s.revenue < 0 then 'negative revenue'
            else null
        end as audit_notes,

        current_timestamp() as record_inserted_at

    from source s
    left join fx_rates fx on s.date::date = fx.date and fx.currency = 'USD'
)

select * from cleaned
