{{ config(materialized='incremental', unique_key='ad_id') }}

with base as (
    select
        tiktok_ads_id as ad_id,
        ad_group,
        campaign_name,
        date,
        country,
        device,
        gender,
        placement,
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
        roas,
        is_valid,
        audit_notes,
        record_inserted_at,
        'tiktok' as platform,
        'social' as channel
    from {{ ref('stg_tiktok_ads') }}
)

select * from base
