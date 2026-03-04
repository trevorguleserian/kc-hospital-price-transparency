{{
  config(
    materialized='view',
    schema='staging',
  )
}}

-- JSON rates: pt_json_registry has no preamble_kv; use source_file_name as hospital identifier (deterministic).
with source as (
  select * from {{ source('pt_analytics', 'pt_json_extracted_rates') }}
),

canonical as (
  select
    source_file_name as hospital_name,
    to_hex(md5(lower(trim(source_file_name)))) as hospital_id,
    billing_code,
    billing_code_type,
    description,
    payer as payer_name,
    plan as plan_name,
    coalesce(rate_type, 'negotiated_rate') as rate_type,
    safe_cast(negotiated_rate as numeric) as rate_amount,
    billing_class,
    cast(null as numeric) as gross_charge,
    cast(null as numeric) as discounted_cash,
    cast(null as numeric) as minimum,
    cast(null as numeric) as maximum,
    'json' as source_system,
    source_file_name,
    run_id,
    ingested_at,
    raw_rate as raw_rate_json
  from source
)

select * from canonical
