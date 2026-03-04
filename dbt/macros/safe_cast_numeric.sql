{# Safe cast to numeric for aggregations and filters. Dispatches by adapter. #}
{% macro safe_cast_numeric(expression) %}
  {{ return(adapter.dispatch('safe_cast_numeric')(expression)) }}
{% endmacro %}

{% macro default__safe_cast_numeric(expression) %}
  safe_cast({{ expression }} as numeric)
{% endmacro %}

{% macro bigquery__safe_cast_numeric(expression) %}
  safe_cast({{ expression }} as numeric)
{% endmacro %}

{% macro duckdb__safe_cast_numeric(expression) %}
  try_cast({{ expression }} as double)
{% endmacro %}
