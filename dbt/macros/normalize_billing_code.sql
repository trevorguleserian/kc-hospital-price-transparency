{#
  Normalize billing_code for format validation (do not mutate stored source).
  - Trim and uppercase for most types.
  - ICD-10-CM: also strip dots for validation (e.g. A12.34 -> A1234).
  - NDC: strip dashes and spaces for digit-length check.
  Usage: {{ normalize_billing_code_for_validation('billing_code', 'billing_code_type') }}
  type_expr should be the normalized type (UPPER, hyphens replaced with underscores) for consistency.
#}
{% macro normalize_billing_code_for_validation(code_expr, type_expr) %}
  case
    when upper(replace(trim(coalesce(cast({{ type_expr }} as string), '')), '-', '_')) = 'ICD_10_CM'
      then replace(upper(trim(coalesce(cast({{ code_expr }} as string), ''))), '.', '')
    when upper(replace(trim(coalesce(cast({{ type_expr }} as string), '')), '-', '_')) = 'NDC'
      then replace(replace(upper(trim(coalesce(cast({{ code_expr }} as string), ''))), '-', ''), ' ', '')
    else upper(trim(coalesce(cast({{ code_expr }} as string), '')))
  end
{% endmacro %}
