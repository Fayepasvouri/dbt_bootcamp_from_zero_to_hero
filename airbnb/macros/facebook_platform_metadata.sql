{% macro facebook_platform_metadata() %}
    -- Facebook ads platform metadata mapping macro
    -- Enriches Facebook data with platform-specific metadata
    
    'facebook' as platform,
    'social' as channel,
    'social' as platform_type,
    false as is_programmatic,
    'GBP' as currency,
    'GB' as default_country,
    'facebook_ads' as platform_name,
    current_timestamp() as metadata_loaded_at
{% endmacro %}
