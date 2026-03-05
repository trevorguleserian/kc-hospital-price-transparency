{#
  Normalize text for matching: upper, trim, replace punctuation with spaces,
  collapse multiple spaces, remove bracket codes like [1234].
  Returns a single normalized string expression (SQL).
  Usage: {{ normalize_text('column_name') }} or {{ normalize_text("coalesce(payer_name, '')") }}
#}
{% macro normalize_text(expr) %}
  {% if target.type == 'bigquery' %}
    upper(
      trim(
        regexp_replace(
          regexp_replace(
            regexp_replace(
              trim(coalesce(cast({{ expr }} as string), '')),
              r'\[\d+\]', ''
            ),
            r'[^a-zA-Z0-9\s]', ' '
          ),
          r' +', ' '
        )
      )
    )
  {% else %}
    {# DuckDB / generic: regexp_replace(string, pattern, replacement) #}
    upper(
      trim(
        regexp_replace(
          regexp_replace(
            regexp_replace(
              trim(coalesce(cast({{ expr }} as string), '')),
              '\[\d+\]', ''
            ),
            '[^a-zA-Z0-9 ]', ' '
          ),
          ' +', ' '
        )
      )
    )
  {% endif %}
{% endmacro %}
