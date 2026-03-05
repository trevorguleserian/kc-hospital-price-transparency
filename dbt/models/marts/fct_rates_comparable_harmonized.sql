{{
  config(
    materialized='table',
    schema='marts',
    tags=['local'],
  )
}}

/*
  Thin layer: fct_rates_comparable plus payer_family, plan_family from dim_payer_harmonized.
  Same grain as fct_rates_comparable; use for aggregation and comparison by harmonized payer/plan.
  We normalize rate_category (lower/trim) here so downstream filters and tests behave deterministically;
  casing/spacing variants of 'other' cannot bypass exclusion in comparison marts.
*/

select
  f.semantic_charge_sk,
  f.standard_charge_sk,
  f.hospital_id,
  f.hospital_name,
  f.billing_code,
  f.billing_code_type,
  f.description,
  f.payer_name,
  f.plan_name,
  coalesce(h.payer_family, trim(coalesce(f.payer_name, ''))) as payer_family,
  coalesce(h.plan_family, trim(coalesce(f.plan_name, ''))) as plan_family,
  lower(trim(cast(f.rate_category as string))) as rate_category,
  f.rate_amount,
  f.rate_unit,
  f.comparability_key,
  f.is_comparable,
  f.comparability_reason,
  f.source_system,
  f.source_file_name,
  f.ingested_at,
  f.contracting_method
from {{ ref('fct_rates_comparable') }} f
left join {{ ref('dim_payer_harmonized') }} h
  on {{ normalize_text('f.payer_name') }} = h.payer_name_norm
  and {{ normalize_text('f.plan_name') }} = h.plan_name_norm
