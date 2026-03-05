{{
  config(
    materialized='incremental',
    schema='marts',
    tags=['local'],
    partition_by={
      'field': 'ingested_at',
      'data_type': 'timestamp',
      'granularity': 'day',
    },
    cluster_by=['hospital_id', 'billing_code', 'payer_name'],
    unique_key='standard_charge_sk',
    incremental_strategy='merge',
  )
}}

{% if var('execution_mode', 'bq') == 'local' %}
with unioned as (
  select * from {{ ref('int_standard_charges_base') }}
),

with_wide as (
  select
    u.*,
    cast(null as string) as billing_class,
    cast(null as string) as run_id,
    case when upper(u.rate_type) = 'GROSS_CHARGE' then u.rate_amount end as gross_charge,
    case when upper(u.rate_type) in ('CASH', 'DISCOUNTED_CASH_PRICE') then u.rate_amount end as discounted_cash,
    case when upper(u.rate_type) = 'MINIMUM' then u.rate_amount end as minimum,
    case when upper(u.rate_type) = 'MAXIMUM' then u.rate_amount end as maximum
  from unioned u
),

with_dim as (
  select
    u.standard_charge_sk,
    u.hospital_name,
    coalesce(d.hospital_id, {{ dbt_utils.generate_surrogate_key(['u.source_file_name']) }}) as hospital_id,
    u.billing_code,
    u.billing_code_type,
    u.description,
    u.payer_name,
    u.plan_name,
    u.rate_type,
    u.rate_amount,
    u.billing_class,
    u.gross_charge,
    u.discounted_cash,
    u.minimum,
    u.maximum,
    case
      when lower(trim(coalesce(u.source_system, ''))) = 'json' then 'json'
      when lower(trim(coalesce(u.source_system, ''))) in ('csv_wide', 'csv_tall') then lower(trim(u.source_system))
      else 'csv_wide'
    end as source_system,
    u.source_file_name,
    u.run_id,
    u.ingested_at,
    u.raw_rate_json
  from with_wide u
  left join {{ ref('dim_hospital') }} d using (source_file_name)
  qualify row_number() over(partition by u.standard_charge_sk order by u.ingested_at desc) = 1
),

fct as (
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
  from with_dim
)

select * from fct
{% else %}
with unioned as (
  select * from {{ ref('int_standard_charges_base') }}
  {% if is_incremental() %}
  where ingested_at > (select coalesce(max(ingested_at), timestamp('1970-01-01')) from {{ this }})
  {% endif %}
),

with_dim as (
  select
    u.standard_charge_sk,
    u.hospital_name,
    coalesce(d.hospital_id, {{ dbt_utils.generate_surrogate_key(['u.source_file_name']) }}) as hospital_id,
    u.billing_code,
    u.billing_code_type,
    u.description,
    u.payer_name,
    u.plan_name,
    u.rate_type,
    u.rate_amount,
    u.billing_class,
    u.gross_charge,
    u.discounted_cash,
    u.minimum,
    u.maximum,
    u.source_system,
    u.source_file_name,
    u.run_id,
    u.ingested_at,
    u.raw_rate_json
  from unioned u
  left join {{ ref('dim_hospital') }} d using (source_file_name)
  qualify row_number() over(partition by u.standard_charge_sk order by u.ingested_at desc) = 1
),

fct as (
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
  from with_dim
)

select * from fct
{% endif %}
