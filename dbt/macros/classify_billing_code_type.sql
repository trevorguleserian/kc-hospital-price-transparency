{#
  Classify billing_code into: CPT, HCPCS, NDC, REVENUE, ICD-10-PCS, UNKNOWN.
  Heuristics: CPT = exactly 5 digits; HCPCS = 1 letter + 4 digits; NDC = 10/11 digits (dashes optional);
  REVENUE = 3-4 digits (and not CPT). Order: more specific first. Adapter-agnostic.
#}
{% macro classify_billing_code_type(billing_code_expr) %}
  {% set code = "trim(coalesce(cast(" ~ billing_code_expr ~ " as varchar), ''))" %}
  {% set code_clean = "replace(replace(" ~ code ~ ", '-', ''), ' ', '')" %}
  case
    when regexp_replace({{ code_clean }}, '^[0-9]{5}$', '') = '' then 'CPT'
    when length({{ code_clean }}) = 5 and regexp_replace({{ code_clean }}, '^[A-Za-z][0-9]{4}$', '') = '' then 'HCPCS'
    when length({{ code_clean }}) in (10, 11) and regexp_replace({{ code_clean }}, '^[0-9]+$', '') = '' then 'NDC'
    when regexp_replace({{ code_clean }}, '^[0-9]{3}$', '') = '' then 'REVENUE'
    when regexp_replace({{ code_clean }}, '^[0-9]{4}$', '') = '' then 'REVENUE'
    when length({{ code_clean }}) = 7 and regexp_replace({{ code_clean }}, '^[A-Za-z][0-9A-Za-z]{6}$', '') = '' then 'ICD-10-PCS'
    else 'UNKNOWN'
  end
{% endmacro %}

{#
  Use inferred billing_code_type only when existing is UNKNOWN (or null/empty).
  Pass the existing type expression and the billing_code expression.
#}
{% macro infer_billing_code_type_if_unknown(billing_code_expr, existing_type_expr) %}
  case
    when lower(trim(coalesce(cast({{ existing_type_expr }} as varchar), ''))) in ('unknown', '') then {{ classify_billing_code_type(billing_code_expr) }}
    else trim(coalesce(cast({{ existing_type_expr }} as varchar), ''))
  end
{% endmacro %}
