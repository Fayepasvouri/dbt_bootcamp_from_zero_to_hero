{% macro dynamic_union(models_list) %}
    -- Dynamic union macro that combines multiple models without hardcoding
    -- models_list: list of model names to union
    
    {% if models_list | length == 0 %}
        {{ exceptions.raise_compiler_error("models_list cannot be empty") }}
    {% endif %}

    {% set models_queries = [] %}
    
    {% for model_name in models_list %}
        {% set query %}
            select * from {{ ref(model_name) }}
        {% endset %}
        {% do models_queries.append(query) %}
    {% endfor %}

    {% for query in models_queries %}
        {% if loop.first %}
            {{ query }}
        {% else %}
            union all
            {{ query }}
        {% endif %}
    {% endfor %}
{% endmacro %}
