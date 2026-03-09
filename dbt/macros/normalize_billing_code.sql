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


{#-
  Normalize billing_code for storage in app-facing marts (removes extra leading digits for CPT, etc.).
  CPT: trim, then use last 5 digits so "199213" -> "99213". Other types: trim only.
  Usage: {{ normalize_billing_code_for_storage('billing_code', 'billing_code_type') }}
-#}
{% macro normalize_billing_code_for_storage(code_expr, type_expr) %}
  case
    when upper(replace(trim(coalesce(cast({{ type_expr }} as string), '')), '-', '_')) = 'CPT'
      then
        case
          when length(trim(cast({{ code_expr }} as string))) >= 5
          then substr(trim(cast({{ code_expr }} as string)), greatest(1, length(trim(cast({{ code_expr }} as string))) - 4), 5)
          else trim(cast({{ code_expr }} as string))
        end
    else trim(cast({{ code_expr }} as string))
  end
{% endmacro %}
