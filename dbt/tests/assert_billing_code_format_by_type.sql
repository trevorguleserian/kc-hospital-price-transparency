/*
  Singular test: fail if any dim_procedure row has a billing_code that does not match
  the expected format for its billing_code_type. Enforces structural validity by code family.
  Hard-enforced types only: CPT, HCPCS, DRG, MS_DRG, APR_DRG, TRIS_DRG, APC, EAPG, HIPPS,
  ICD_10_PCS, ICD_10_CM, REVENUE, RC. NDC, CDM, and UNKNOWN are excluded.
*/
with base as (
  select
    billing_code,
    billing_code_type,
    upper(replace(trim(coalesce(cast(billing_code_type as string), '')), '-', '_')) as type_norm,
    {{ normalize_billing_code_for_validation('billing_code', 'billing_code_type') }} as normalized_billing_code
  from {{ ref('dim_procedure') }}
  where upper(replace(trim(coalesce(cast(billing_code_type as string), '')), '-', '_')) in (
    'CPT', 'HCPCS', 'DRG', 'MS_DRG', 'APR_DRG', 'TRIS_DRG', 'APC', 'EAPG', 'HIPPS',
    'ICD_10_PCS', 'ICD_10_CM', 'REVENUE', 'RC'
  )
)
select
  billing_code,
  billing_code_type,
  normalized_billing_code
from base
where
  (type_norm = 'CPT' and not regexp_contains(normalized_billing_code, r'^[0-9]{5}$'))
  or (type_norm = 'HCPCS' and not regexp_contains(normalized_billing_code, r'^[A-Z0-9]{5}$'))
  or (type_norm = 'DRG' and not regexp_contains(normalized_billing_code, r'^[0-9]{3}$'))
  or (type_norm = 'MS_DRG' and not regexp_contains(normalized_billing_code, r'^[0-9]{3}$'))
  or (type_norm = 'APR_DRG' and not regexp_contains(normalized_billing_code, r'^[0-9]{3}$'))
  or (type_norm = 'TRIS_DRG' and not regexp_contains(normalized_billing_code, r'^[0-9]{3}$'))
  or (type_norm = 'APC' and not regexp_contains(normalized_billing_code, r'^[0-9]{4}$'))
  or (type_norm = 'EAPG' and not regexp_contains(normalized_billing_code, r'^[0-9]{4}$'))
  or (type_norm = 'HIPPS' and not regexp_contains(normalized_billing_code, r'^[A-Z0-9]{5}$'))
  or (type_norm = 'ICD_10_PCS' and not regexp_contains(normalized_billing_code, r'^[A-Z0-9]{7}$'))
  or (type_norm = 'ICD_10_CM' and not regexp_contains(normalized_billing_code, r'^[A-Z0-9]{3,7}$'))
  or (type_norm = 'REVENUE' and not regexp_contains(normalized_billing_code, r'^[0-9]{4}$'))
  or (type_norm = 'RC' and not regexp_contains(normalized_billing_code, r'^[0-9]{4}$'))
