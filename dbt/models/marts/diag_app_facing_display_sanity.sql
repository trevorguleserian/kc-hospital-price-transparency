{{
  config(
    materialized='view',
    schema='marts',
    tags=['diagnostic'],
  )
}}

/*
  Diagnostic: confirm app-facing data is display-ready.
  - hospital_name_clean: human-readable (not 32-char hash).
  - billing_code 99213: normalized as '99213' in comparison agg.
  Run: dbt run --select diag_app_facing_display_sanity, then query this view.
*/
with hospital_sample as (
  select
    'hospital_name_clean' as check_type,
    hospital_id as key_value,
    hospital_name_clean as detail,
    (length(trim(coalesce(hospital_name_clean, ''))) != 32
     or not regexp_contains(lower(trim(coalesce(hospital_name_clean, ''))), r'^[0-9a-f]{32}$')) as ok
  from {{ ref('dim_hospital') }}
  limit 10
),

billing_99213_sample as (
  select
    'billing_code_99213' as check_type,
    cast(billing_code as string) as key_value,
    cast(count(*) as string) || ' rows' as detail,
    trim(cast(billing_code as string)) = '99213' as ok
  from {{ ref('agg_hospital_procedure_compare') }}
  where trim(cast(billing_code as string)) = '99213'
     or trim(cast(billing_code as string)) like '%99213%'
  group by billing_code
  limit 10
)

select * from hospital_sample
union all
select * from billing_99213_sample
