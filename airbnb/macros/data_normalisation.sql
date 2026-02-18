{% macro data_normalisation(
    source_model,
    platform_name,
    id_column,
    campaign_column,
    adset_column,
    date_column,
    country_column,
    device_column,
    impressions_column,
    clicks_column,
    conversions_column,
    spend_column,
    revenue_column
) %}

select
    {{ id_column }}              as platform_id,
    '{{ platform_name }}'        as platform,
    {{ campaign_column }}        as campaign_name,
    {{ adset_column }}           as adset_name,
    {{ date_column }}            as date,
    {{ country_column }}         as country,
    {{ device_column }}          as device,
    {{ impressions_column }}     as impressions,
    {{ clicks_column }}          as clicks,
    {{ conversions_column }}     as conversions,
    {{ spend_column }}           as spend,
    {{ revenue_column }}         as revenue

from {{ ref(source_model) }}

{% endmacro %}
