{% macro fx_conversion(amount_column, currency, date_column='date') %}
    -- FX conversion macro that converts amounts to GBP using dim_fx_rates
    -- Parameters: amount_column, currency (source currency), date_column
    
    {{ safe_divide(amount_column, 'fx_rates.rate_to_gbp', amount_column) }}
{% endmacro %}
