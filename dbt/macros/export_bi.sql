{% macro export_relation_to_parquet(relation, out_path) %}
  {%- call statement('export_parquet') -%}
    copy {{ relation }} to '{{ out_path }}' (format parquet)
  {%- endcall -%}
{% endmacro %}


{% macro export_relation_to_csv(relation, out_path) %}
  {%- call statement('export_csv') -%}
    copy {{ relation }} to '{{ out_path }}' (format csv, header true)
  {%- endcall -%}
{% endmacro %}


{% macro export_bi_outputs() %}
  {% if target.name != 'local_duckdb' %}
    {% do log("export_bi_outputs: skipped (target is not local_duckdb)", info=true) %}
    {{ return('') }}
  {% endif %}

  {% set relations_to_export = [
    ('dim_hospital', ref('dim_hospital')),
    ('dim_procedure', ref('dim_procedure')),
    ('dim_payer', ref('dim_payer')),
    ('fct_standard_charges_semantic', ref('fct_standard_charges_semantic'))
  ] %}

  {% for name, rel in relations_to_export %}
    {{ export_relation_to_parquet(rel, 'exports/' ~ name ~ '.parquet') }}
    {{ export_relation_to_csv(rel, 'exports/' ~ name ~ '.csv') }}
  {% endfor %}

  {% do log("export_bi_outputs: exported " ~ relations_to_export | length * 2 ~ " file(s) (parquet + csv per relation)", info=true) %}

  {% for name, rel in relations_to_export %}
    {% set stmt_name = 'row_count_' ~ name %}
    {% call statement(stmt_name, fetch_result=True) %}
      select count(*) as n from {{ rel }}
    {% endcall %}
    {% set tbl = load_result(stmt_name).table %}
    {% set row_count = tbl.rows[0][0] if tbl.rows else 0 %}
    {% do log("  " ~ name ~ ": " ~ row_count ~ " rows", info=true) %}
  {% endfor %}
{% endmacro %}
