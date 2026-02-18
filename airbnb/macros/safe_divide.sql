{% macro safe_divide(numerator, denominator, default_value=0) %}
    -- Safe division macro that handles division by zero
    case 
        when {{ denominator }} = 0 or {{ denominator }} is null 
        then {{ default_value }}
        else {{ numerator }} / {{ denominator }}
    end
{% endmacro %}
