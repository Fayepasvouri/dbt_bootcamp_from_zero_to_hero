{% macro incremental_where(lookback_days=7) %}
    -- Incremental logic macro for efficient incremental models
    {% if execute %}
        {% if flags.FULL_REFRESH %}
            -- Full refresh: no where clause
            {}
        {% else %}
            -- Incremental run: only fetch recent data with lookback period
            where date >= dateadd(day, -{{ lookback_days }}, current_date)
        {% endif %}
    {% endif %}
{% endmacro %}
