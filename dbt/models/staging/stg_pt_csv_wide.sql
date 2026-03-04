{{
  config(
    materialized='view',
    schema='staging',
  )
}}

/*
  CSV wide: one row per procedure with many columns; unpivot known rate columns into tall rows.
  hospital_name / hospital_id from pt_csv_registry.preamble_kv (LEFT JOIN on source_file_name).
*/
with source as (
  select
    t.source_file_name,
    t.ingested_at,
    t.raw
  from {{ source('pt_analytics', 'pt_csv_raw_wide') }} t
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

base as (
  select
    source_file_name,
    ingested_at,
    raw,
    hospital_name_from_reg,
    coalesce(
      json_value(raw, '$.description'),
      json_value(raw, '$.procedure_description'),
      json_value(raw, '$.Description')
    ) as description,
    coalesce(
      json_value(raw, '$.code'),
      json_value(raw, '$.billing_code'),
      json_value(raw, '$.service_code'),
      json_value(raw, '$.CPT'),
      json_value(raw, '$.HCPCS')
    ) as billing_code,
    coalesce(
      json_value(raw, '$.billing_code_type'),
      json_value(raw, '$.code_type')
    ) as billing_code_type,
    coalesce(nullif(trim(json_value(raw, '$.hospital_name')), ''), hospital_name_from_reg) as hospital_name,
    coalesce(
      json_value(raw, '$.billing_class'),
      json_value(raw, '$.setting')
    ) as billing_class,
    safe_cast(json_value(raw, '$.gross_charge') as numeric) as gross_charge,
    safe_cast(json_value(raw, '$.discounted_cash') as numeric) as discounted_cash,
    safe_cast(json_value(raw, '$.min') as numeric) as minimum,
    safe_cast(json_value(raw, '$.max') as numeric) as maximum,
    safe_cast(json_value(raw, '$.negotiated_dollar') as numeric) as negotiated_dollar,
    safe_cast(json_value(raw, '$.negotiated_percentage') as numeric) as negotiated_percentage,
    safe_cast(json_value(raw, '$.estimated_amount') as numeric) as estimated_amount
  from source_with_reg
),

unpivot_rates as (
  select
    source_file_name,
    ingested_at,
    raw,
    description,
    billing_code,
    billing_code_type,
    hospital_name,
    billing_class,
    gross_charge,
    discounted_cash,
    minimum,
    maximum,
    'negotiated_dollar' as rate_type,
    negotiated_dollar as rate_amount,
    cast(null as string) as payer_name,
    cast(null as string) as plan_name
  from base where negotiated_dollar is not null
  union all
  select
    source_file_name, ingested_at, raw, description, billing_code, billing_code_type,
    hospital_name, billing_class, gross_charge, discounted_cash, minimum, maximum,
    'negotiated_percentage', negotiated_percentage, cast(null as string), cast(null as string)
  from base where negotiated_percentage is not null
  union all
  select
    source_file_name, ingested_at, raw, description, billing_code, billing_code_type,
    hospital_name, billing_class, gross_charge, discounted_cash, minimum, maximum,
    'estimated_amount', estimated_amount, cast(null as string), cast(null as string)
  from base where estimated_amount is not null
),

canonical as (
  select
    hospital_name,
    to_hex(md5(lower(trim(coalesce(nullif(trim(hospital_name), ''), source_file_name))))) as hospital_id,
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
    'csv_wide' as source_system,
    source_file_name,
    cast(null as string) as run_id,
    ingested_at,
    raw as raw_rate_json
  from unpivot_rates
)

select * from canonical
