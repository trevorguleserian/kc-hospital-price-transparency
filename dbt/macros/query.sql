{% macro query(sql) %}
  {# Run ad-hoc SQL and print results. Usage: dbt run-operation query --args "{\"sql\": \"select 1 as x\"}" #}
  {% if execute and sql is not none and sql | length > 0 %}
    {% set result = run_query(sql) %}
    {% if result is not none and result.rows | length > 0 %}
      {% do log(result.print_table(), info=true) %}
    {% elif result is not none %}
      {% do log("(0 rows)", info=true) %}
    {% else %}
      {% do log("Query returned no result.", info=true) %}
    {% endif %}
  {% endif %}
{% endmacro %}
