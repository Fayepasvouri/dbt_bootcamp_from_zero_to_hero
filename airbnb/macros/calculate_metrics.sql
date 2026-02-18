{% macro calculate_metrics() %}

    {{ safe_divide('clicks', 'impressions', 0) }} * 100 as ctr_percent,

    {{ safe_divide('spend', 'clicks', 0) }} as cpc,

    {{ safe_divide('spend', 'conversions', 0) }} as cpa,

    {{ safe_divide('revenue', 'spend', 0) }} as roas

{% endmacro %}
