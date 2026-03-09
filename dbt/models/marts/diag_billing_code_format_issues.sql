{{
  config(
    materialized='view',
    schema='marts',
    tags=['diagnostic'],
  )
}}

/*
  Diagnostic: rows where billing_code does not match expected format for its billing_code_type.
  Normalizes code (trim, upper; ICD-10-CM dot-stripped, NDC dash/space stripped) for validation only.
  CDM and UNKNOWN are excluded from validation. NDC is warning-only (flagged here but not hard-failed in test).
  Run: dbt run --select diag_billing_code_format_issues, then query this view.
*/
with base as (
  select
    billing_code,
    billing_code_type
  from {{ ref('fct_standard_charges_semantic') }}
  where billing_code is not null and billing_code_type is not null
),

with_norm as (
  select
    billing_code,
    billing_code_type,
    upper(replace(trim(cast(billing_code_type as string)), '-', '_')) as type_norm,
    {{ normalize_billing_code_for_validation('billing_code', 'billing_code_type') }} as normalized_billing_code
  from base
),

with_reason as (
  select
    billing_code,
    billing_code_type,
    normalized_billing_code,
    case
      when type_norm = 'CPT' and not regexp_contains(normalized_billing_code, r'^[0-9]{5}$') then 'cpt_expected_5_digits'
      when type_norm = 'HCPCS' and not regexp_contains(normalized_billing_code, r'^[A-Z0-9]{5}$') then 'hcpcs_expected_5_alphanum'
      when type_norm = 'DRG' and not regexp_contains(normalized_billing_code, r'^[0-9]{3}$') then 'drg_expected_3_digits'
      when type_norm = 'MS_DRG' and not regexp_contains(normalized_billing_code, r'^[0-9]{3}$') then 'ms_drg_expected_3_digits'
      when type_norm = 'APR_DRG' and not regexp_contains(normalized_billing_code, r'^[0-9]{3}$') then 'apr_drg_expected_3_digits'
      when type_norm = 'TRIS_DRG' and not regexp_contains(normalized_billing_code, r'^[0-9]{3}$') then 'tris_drg_expected_3_digits'
      when type_norm = 'APC' and not regexp_contains(normalized_billing_code, r'^[0-9]{4}$') then 'apc_expected_4_digits'
      when type_norm = 'EAPG' and not regexp_contains(normalized_billing_code, r'^[0-9]{4}$') then 'eapg_expected_4_digits'
      when type_norm = 'HIPPS' and not regexp_contains(normalized_billing_code, r'^[A-Z0-9]{5}$') then 'hipps_expected_5_alphanum'
      when type_norm = 'ICD_10_PCS' and not regexp_contains(normalized_billing_code, r'^[A-Z0-9]{7}$') then 'icd_10_pcs_expected_7_alphanum'
      when type_norm = 'ICD_10_CM' and not regexp_contains(normalized_billing_code, r'^[A-Z0-9]{3,7}$') then 'icd_10_cm_expected_3_to_7_alphanum'
      when type_norm = 'REVENUE' and not regexp_contains(normalized_billing_code, r'^[0-9]{4}$') then 'revenue_expected_4_digits'
      when type_norm = 'RC' and not regexp_contains(normalized_billing_code, r'^[0-9]{4}$') then 'rc_expected_4_digits'
      when type_norm = 'NDC' and length(normalized_billing_code) not in (10, 11) then 'ndc_not_10_or_11_digits'
      else null
    end as issue_reason
  from with_norm
  where upper(replace(trim(cast(billing_code_type as string)), '-', '_')) not in ('CDM', 'UNKNOWN')
)

select
  billing_code,
  billing_code_type,
  any_value(normalized_billing_code) as normalized_billing_code,
  issue_reason,
  count(*) as row_count
from with_reason
where issue_reason is not null
group by billing_code, billing_code_type, issue_reason
order by row_count desc
