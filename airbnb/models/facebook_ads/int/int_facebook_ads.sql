-- depends_on: {{ ref('stg_facebook_ads') }}

with base as (

    {{ data_normalisation(
        source_model='stg_facebook_ads',
        platform_name='facebook',
        id_column='ad_id',
        campaign_column='campaign_name',
        adset_column='adset_name',
        date_column='date',
        country_column='country',
        device_column='device',
        impressions_column='impressions',
        clicks_column='clicks',
        conversions_column='conversions',
        spend_column='spend',
        revenue_column='revenue'
    ) }}

)

select
    *,
    {{ calculate_metrics() }}
from base
