{#
  Centralized billing code normalization and validation.
  BigQuery-compatible SQL only.
  Assumptions by type (documented):
  - CPT: exactly 5 digits. Malformed leading digits (e.g. 199213, 999213) normalize to last 5 digits only for CPT.
  - HCPCS: exactly 5 alphanumeric (A-Z, 0-9).
  - NDC: digits only after stripping punctuation; canonical 10 or 11 digits (NDC format: 4-4-2 or 5-3-2 or 5-4-1).
  - REVENUE / RC: 4 digits (CMS revenue codes). CDM: 3 or 4 digits (charge code); document assumption.
  - ICD_10_PCS: 7 alphanumeric.
  - ICD_10_CM: 3-7 alphanumeric after removing dots; allow valid style.
  - DRG / MS_DRG / APR_DRG / TRIS_DRG: 3 digits.
  - HIPPS: 5 alphanumeric.
  - APC / EAPG: 4 digits.
  - CDM / UNKNOWN: trim only; no strict validation (is_valid = true, issue_reason = null).
  Preserve leading zeroes where valid (e.g. CPT 01234 stays 01234 after normalization).
#}

{% macro _billing_code_type_norm(type_expr) %}
  upper(replace(trim(coalesce(cast({{ type_expr }} as string), '')), '-', '_'))
{% endmacro %}

{#
  Normalize billing_code for storage. CPT only: if length >= 5, use last 5 digits (handles 199213 -> 99213).
  Other types: trim; HCPCS/ICD/NDC get uppercase and punctuation stripped per type.
  Preserve leading zeroes for CPT (e.g. 01234).
#}
{% macro normalize_billing_code_for_storage(code_expr, type_expr) %}
  case
    when {{ _billing_code_type_norm(type_expr) }} = 'CPT' then
      case
        when length(trim(cast({{ code_expr }} as string))) >= 5
        then substr(trim(cast({{ code_expr }} as string)), greatest(1, length(trim(cast({{ code_expr }} as string))) - 4), 5)
        else trim(cast({{ code_expr }} as string))
      end
    when {{ _billing_code_type_norm(type_expr) }} = 'HCPCS' then upper(trim(cast({{ code_expr }} as string)))
    when {{ _billing_code_type_norm(type_expr) }} = 'ICD_10_CM' then replace(upper(trim(cast({{ code_expr }} as string))), '.', '')
    when {{ _billing_code_type_norm(type_expr) }} = 'ICD_10_PCS' then upper(trim(cast({{ code_expr }} as string)))
    when {{ _billing_code_type_norm(type_expr) }} = 'NDC' then replace(replace(replace(trim(cast({{ code_expr }} as string)), '-', ''), ' ', ''), '.', '')
    else trim(cast({{ code_expr }} as string))
  end
{% endmacro %}

{#
  Returns SQL expression for the issue reason (string or null). Use after normalizing; expects code_expr/type_expr to be the raw columns.
  Uses normalized form inline for validation.
#}
{% macro billing_code_issue_reason(code_expr, type_expr) %}
  case
    when {{ _billing_code_type_norm(type_expr) }} = 'CPT' and not regexp_contains(
      case when length(trim(cast({{ code_expr }} as string))) >= 5 then substr(trim(cast({{ code_expr }} as string)), greatest(1, length(trim(cast({{ code_expr }} as string))) - 4), 5) else trim(cast({{ code_expr }} as string)) end,
      r'^[0-9]{5}$') then 'cpt_expected_5_digits'
    when {{ _billing_code_type_norm(type_expr) }} = 'HCPCS' and not regexp_contains(upper(trim(cast({{ code_expr }} as string))), r'^[A-Z0-9]{5}$') then 'hcpcs_expected_5_alphanum'
    when {{ _billing_code_type_norm(type_expr) }} = 'NDC' and length(replace(replace(replace(trim(cast({{ code_expr }} as string)), '-', ''), ' ', ''), '.', '')) not in (10, 11) then 'ndc_not_10_or_11_digits'
    when {{ _billing_code_type_norm(type_expr) }} = 'REVENUE' and not regexp_contains(trim(cast({{ code_expr }} as string)), r'^[0-9]{3,4}$') then 'revenue_expected_3_or_4_digits'
    when {{ _billing_code_type_norm(type_expr) }} = 'RC' and not regexp_contains(trim(cast({{ code_expr }} as string)), r'^[0-9]{3,4}$') then 'rc_expected_3_or_4_digits'
    when {{ _billing_code_type_norm(type_expr) }} = 'CDM' and not regexp_contains(trim(cast({{ code_expr }} as string)), r'^[0-9]{3,4}$') then 'cdm_expected_3_or_4_digits'
    when {{ _billing_code_type_norm(type_expr) }} = 'ICD_10_PCS' and not regexp_contains(upper(trim(cast({{ code_expr }} as string))), r'^[A-Z0-9]{7}$') then 'icd_10_pcs_expected_7_alphanum'
    when {{ _billing_code_type_norm(type_expr) }} = 'ICD_10_CM' and not regexp_contains(replace(upper(trim(cast({{ code_expr }} as string))), '.', ''), r'^[A-Z0-9]{3,7}$') then 'icd_10_cm_expected_3_to_7_alphanum'
    when {{ _billing_code_type_norm(type_expr) }} in ('DRG', 'MS_DRG', 'APR_DRG', 'TRIS_DRG') and not regexp_contains(trim(cast({{ code_expr }} as string)), r'^[0-9]{3}$') then 'drg_expected_3_digits'
    when {{ _billing_code_type_norm(type_expr) }} = 'HIPPS' and not regexp_contains(upper(trim(cast({{ code_expr }} as string))), r'^[A-Z0-9]{5}$') then 'hipps_expected_5_alphanum'
    when {{ _billing_code_type_norm(type_expr) }} in ('APC', 'EAPG') and not regexp_contains(trim(cast({{ code_expr }} as string)), r'^[0-9]{4}$') then 'apc_eapg_expected_4_digits'
    when {{ _billing_code_type_norm(type_expr) }} not in ('CPT', 'HCPCS', 'NDC', 'REVENUE', 'RC', 'CDM', 'ICD_10_PCS', 'ICD_10_CM', 'DRG', 'MS_DRG', 'APR_DRG', 'TRIS_DRG', 'HIPPS', 'APC', 'EAPG', 'UNKNOWN', '8', 'ICD') then 'unknown_code_type'
    else null
  end
{% endmacro %}

{#
  Returns SQL boolean: true if the (raw) code passes validation for its type.
  CDM and UNKNOWN are treated as valid (no strict rule).
#}
{% macro is_valid_billing_code(code_expr, type_expr) %}
  ({{ billing_code_issue_reason(code_expr, type_expr) }} is null)
{% endmacro %}
