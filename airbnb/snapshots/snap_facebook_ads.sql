{% snapshot facebook_ads_snapshot %}

{{ config(
    target_schema='snapshots',
    unique_key='ad_id',
    strategy='check',
    check_cols=['clicks','impressions','conversions','spend','revenue']
) }}

select
    ad_id,
    campaign_name,
    adset_name,
    ad_name,
    date::date as date,
    country,
    device,
    coalesce(impressions,0)::int as impressions,
    coalesce(clicks,0)::int as clicks,
    coalesce(conversions,0)::int as conversions,
    coalesce(spend,0)::float as spend,
    coalesce(revenue,0)::float as revenue,
    'facebook_ads' as platform,
    'social' as channel
from {{ ref('int_facebook_ads') }}

{% endsnapshot %}
