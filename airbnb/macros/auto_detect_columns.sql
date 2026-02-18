{% macro auto_detect_columns(config_dict) %}
    -- Auto-detect columns macro that maps source columns to standardized names via config
    -- config_dict: dictionary with standard column names as keys and source columns as values
    -- Example: {'impressions': 'impression_count', 'clicks': 'click_count'}
    
    {% for standard_col, source_col in config_dict.items() %}
        {% if source_col is not none %}
            {{ source_col }} as {{ standard_col }}
        {% else %}
            null as {{ standard_col }}
        {% endif %}
        {% if not loop.last %},{% endif %}
    {% endfor %}
{% endmacro %}
