{{
  config(
    materialized='view',
    schema='staging',
  )
}}

-- CSV tall: one row per rate; raw JSON has row3 header names as keys.
-- hospital_name / hospital_id derived from pt_csv_registry.preamble_kv (LEFT JOIN on source_file_name).
with source as (
  select
    t.source_file_name,
    t.ingested_at,
    t.raw
  from {{ source('pt_analytics', 'pt_csv_raw_tall') }} t
),
registry as (
  select
    source_file_name,
    preamble_kv
  from {{ source('pt_analytics', 'pt_csv_registry') }}
),
source_with_reg as (
  select
    s.source_file_name,
    s.ingested_at,
    s.raw,
    coalesce(
      nullif(trim(safe_cast(json_value(r.preamble_kv, '$.hospital_name') as string)), ''),
      nullif(trim(safe_cast(json_value(r.preamble_kv, '$.hospital') as string)), ''),
      nullif(trim(safe_cast(json_value(r.preamble_kv, '$.facility_name') as string)), ''),
      s.source_file_name
    ) as hospital_name_from_reg
  from source s
  left join registry r on s.source_file_name = r.source_file_name
),
parsed as (
  select
    source_file_name,
    ingested_at,
    raw,
    hospital_name_from_reg,
    coalesce(json_value(raw, '$.hospital_name'), hospital_name_from_reg) as hospital_name_row,
    coalesce(json_value(raw, '$.billing_code'), json_value(raw, '$.code'), json_value(raw, '$.service_code')) as billing_code,
    coalesce(json_value(raw, '$.billing_code_type'), json_value(raw, '$.code_type')) as billing_code_type,
    coalesce(json_value(raw, '$.description'), json_value(raw, '$.procedure_description')) as description,
    coalesce(json_value(raw, '$.payer_name'), json_value(raw, '$.payer')) as payer_name,
    coalesce(json_value(raw, '$.plan_name'), json_value(raw, '$.plan')) as plan_name,
    coalesce(json_value(raw, '$.rate_type'), json_value(raw, '$.standard_charge_type'), 'estimated_amount') as rate_type,
    safe_cast(coalesce(json_value(raw, '$.estimated_amount'), json_value(raw, '$.negotiated_rate'), json_value(raw, '$.rate'), json_value(raw, '$.amount')) as numeric) as rate_amount,
    coalesce(json_value(raw, '$.billing_class'), json_value(raw, '$.setting')) as billing_class,
    safe_cast(json_value(raw, '$.gross_charge') as numeric) as gross_charge,
    safe_cast(json_value(raw, '$.discounted_cash') as numeric) as discounted_cash,
    safe_cast(coalesce(json_value(raw, '$.minimum'), json_value(raw, '$.min')) as numeric) as minimum,
    safe_cast(coalesce(json_value(raw, '$.maximum'), json_value(raw, '$.max')) as numeric) as maximum
  from source_with_reg
),

canonical as (
  select
    coalesce(nullif(trim(hospital_name_row), ''), hospital_name_from_reg) as hospital_name,
    to_hex(md5(lower(trim(coalesce(nullif(trim(hospital_name_row), ''), hospital_name_from_reg, source_file_name))))) as hospital_id,
    billing_code,
    billing_code_type,
    description,
    payer_name,
    plan_name,
    rate_type,
    rate_amount,
    billing_class,
    gross_charge,
    discounted_cash,
    minimum,
    maximum,
    'csv_tall' as source_system,
    source_file_name,
    cast(null as string) as run_id,
    ingested_at,
    raw as raw_rate_json
  from parsed
)

select * from canonical
