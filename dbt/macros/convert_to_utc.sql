{% macro convert_to_utc(column_name) %}
    -- Convert a timestamp column to UTC timezone
    -- Usage in model: {{ convert_to_utc('created_at') }}
    toTimeZone({{ column_name }}, 'UTC')
{% endmacro %}
