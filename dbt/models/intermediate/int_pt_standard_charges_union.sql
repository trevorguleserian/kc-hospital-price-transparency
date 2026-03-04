{{
  config(
    materialized='table',
    schema='intermediate',
  )
}}

with json_rates as (
  select * from {{ ref('stg_pt_json_rates') }}
),
csv_tall as (
  select * from {{ ref('stg_pt_csv_tall') }}
),
csv_wide as (
  select * from {{ ref('stg_pt_csv_wide') }}
),

unioned as (
  select * from json_rates
  union all
  select * from csv_tall
  union all
  select * from csv_wide
),

-- Prepare expressions for surrogate key (BigQuery-safe). Payload fingerprint uses raw_rate_json
-- (canonical column; no "raw" or "record" in union output).
with_key_inputs as (
  select
    *,
    cast(ingested_at as string) as _ingested_at_ts,
    cast(rate_amount as string) as _rate_amount_str,
    cast('' as string) as _rate_unit,
    coalesce(to_json_string(raw_rate_json), '{}') as _payload_fingerprint
  from unioned
),

with_sk as (
  select
    {{ dbt_utils.generate_surrogate_key([
      'source_system',
      'source_file_name',
      '_ingested_at_ts',
      'hospital_id',
      'billing_code',
      'billing_code_type',
      'payer_name',
      'plan_name',
      'rate_type',
      '_rate_amount_str',
      '_rate_unit',
      '_payload_fingerprint'
    ]) }} as standard_charge_sk,
    hospital_name,
    hospital_id,
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
    source_system,
    source_file_name,
    run_id,
    ingested_at,
    raw_rate_json
  from with_key_inputs
),

deduped as (
  select
    standard_charge_sk,
    hospital_name,
    hospital_id,
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
    source_system,
    source_file_name,
    run_id,
    ingested_at,
    raw_rate_json
  from (
    select
      *,
      row_number() over(partition by standard_charge_sk order by ingested_at desc) as rn
    from with_sk
  )
  where rn = 1
)

select * from deduped
