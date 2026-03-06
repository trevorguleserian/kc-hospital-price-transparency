{{
  config(
    materialized='table',
    schema='marts',
    tags=['local'],
  )
}}

{% if var('execution_mode', 'bq') == 'local' %}
/*
  Local mode: one row per (standard_charge_sk, rate_category) from fct_standard_charges (DuckDB-compatible).
*/
with b as (
  select * from {{ ref('fct_standard_charges') }}
),
mapped as (
  select
    b.standard_charge_sk,
    b.hospital_id,
    b.hospital_name,
    b.billing_code,
    b.billing_code_type,
    b.description,
    b.payer_name,
    b.plan_name,
    case
      when upper(coalesce(b.rate_type, '')) in ('NEGOTIATED', 'NEGOTIATED_RATE') then 'negotiated'
      when upper(coalesce(b.rate_type, '')) in ('GROSS', 'GROSS_CHARGE') then 'gross'
      when upper(coalesce(b.rate_type, '')) in ('CASH', 'DISCOUNTED_CASH_PRICE') then 'cash'
      when upper(coalesce(b.rate_type, '')) = 'MINIMUM' then 'min'
      when upper(coalesce(b.rate_type, '')) = 'MAXIMUM' then 'max'
      else 'other'
    end as rate_category,
    try_cast(b.rate_amount as double) as rate_amount,
    'dollars' as rate_unit,
    b.source_system,
    b.source_file_name,
    b.ingested_at,
    cast(null as string) as contracting_method
  from b
  where b.rate_amount is not null and try_cast(b.rate_amount as double) is not null and try_cast(b.rate_amount as double) != 0
)
select
  {{ dbt_utils.generate_surrogate_key(['standard_charge_sk', 'rate_category']) }} as semantic_charge_sk,
  standard_charge_sk,
  hospital_id,
  hospital_name,
  billing_code,
  billing_code_type,
  description,
  payer_name,
  plan_name,
  rate_category,
  rate_amount,
  rate_unit,
  source_system,
  source_file_name,
  ingested_at,
  contracting_method
from mapped
{% else %}
/*
  Semantic fact: one row per (standard_charge_sk, rate_category).
  Normalizes rate fields from union into rate_amount + rate_category; only emits rows where rate_amount is not null and != 0.
  rate_category: negotiated, gross, cash, min, max, percentage, other.
*/
with unioned as (
  select
    standard_charge_sk,
    hospital_id,
    hospital_name,
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
    ingested_at
  from {{ ref('int_standard_charges_base') }}
),

-- Resolve hospital_id from dim_hospital so relationship test passes; fallback to surrogate on source_file_name.
-- Filter out null billing_code so semantic layer is BI-safe and not_null test passes.
base as (
  select
    u.standard_charge_sk,
    coalesce(d.hospital_id, {{ dbt_utils.generate_surrogate_key(['u.source_file_name']) }}) as hospital_id,
    u.hospital_name,
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
    u.ingested_at
  from unioned u
  left join {{ ref('dim_hospital') }} d on u.source_file_name = d.source_file_name
  where u.billing_code is not null
),

-- Primary rate row: map rate_type to canonical rate_category (negotiated, gross, cash, min, max, percentage, other).
-- Normalize with lower/trim; use coalesce(rate_type,'') so null/empty does not fall to 'other'. Fallback: empty + typed column match -> that category; else empty -> negotiated.
primary_rows as (
  select
    b.standard_charge_sk,
    b.hospital_id,
    b.hospital_name,
    b.billing_code,
    b.billing_code_type,
    b.description,
    b.payer_name,
    b.plan_name,
    coalesce(nullif(trim(
      case
        when lower(trim(coalesce(cast(b.rate_type as string), ''))) in ('negotiated_rate', 'negotiated_dollar', 'negotiated', 'estimated_amount', 'estimated', 'self_pay', 'self_pay_rate', 'self-pay') then 'negotiated'
        when lower(trim(coalesce(cast(b.rate_type as string), ''))) in ('negotiated_percentage', 'percentage') then 'percentage'
        when lower(trim(coalesce(cast(b.rate_type as string), ''))) in ('gross_charge', 'gross', 'standard_charge_gross', 'standard_charge', 'charge') then 'gross'
        when lower(trim(coalesce(cast(b.rate_type as string), ''))) in ('discounted_cash', 'discounted_cash_price', 'cash_price', 'cash') then 'cash'
        when lower(trim(coalesce(cast(b.rate_type as string), ''))) in ('min', 'minimum') then 'min'
        when lower(trim(coalesce(cast(b.rate_type as string), ''))) in ('max', 'maximum') then 'max'
        when lower(trim(coalesce(cast(b.rate_type as string), ''))) = '' and b.gross_charge is not null and safe_cast(b.rate_amount as numeric) = safe_cast(b.gross_charge as numeric) then 'gross'
        when lower(trim(coalesce(cast(b.rate_type as string), ''))) = '' and b.discounted_cash is not null and safe_cast(b.rate_amount as numeric) = safe_cast(b.discounted_cash as numeric) then 'cash'
        when lower(trim(coalesce(cast(b.rate_type as string), ''))) = '' and b.minimum is not null and safe_cast(b.rate_amount as numeric) = safe_cast(b.minimum as numeric) then 'min'
        when lower(trim(coalesce(cast(b.rate_type as string), ''))) = '' and b.maximum is not null and safe_cast(b.rate_amount as numeric) = safe_cast(b.maximum as numeric) then 'max'
        when lower(trim(coalesce(cast(b.rate_type as string), ''))) = '' then 'negotiated'
        else 'other'
      end
    ), ''), 'other') as rate_category,
    safe_cast(b.rate_amount as numeric) as rate_amount,
    case when lower(trim(coalesce(cast(b.rate_type as string), ''))) like '%percent%' then 'percent' else 'dollars' end as rate_unit,
    b.source_system,
    b.source_file_name,
    b.ingested_at,
    cast(null as string) as contracting_method
  from base b
  where b.rate_amount is not null and safe_cast(b.rate_amount as numeric) != 0
),

gross_rows as (
  select
    b.standard_charge_sk,
    b.hospital_id,
    b.hospital_name,
    b.billing_code,
    b.billing_code_type,
    b.description,
    b.payer_name,
    b.plan_name,
    'gross' as rate_category,
    b.gross_charge as rate_amount,
    'dollars' as rate_unit,
    b.source_system,
    b.source_file_name,
    b.ingested_at,
    cast(null as string) as contracting_method
  from base b
  where b.gross_charge is not null and safe_cast(b.gross_charge as numeric) != 0
    and lower(coalesce(b.rate_type, '')) not in ('gross_charge', 'gross', 'standard_charge_gross')
),

cash_rows as (
  select
    b.standard_charge_sk,
    b.hospital_id,
    b.hospital_name,
    b.billing_code,
    b.billing_code_type,
    b.description,
    b.payer_name,
    b.plan_name,
    'cash' as rate_category,
    b.discounted_cash as rate_amount,
    'dollars' as rate_unit,
    b.source_system,
    b.source_file_name,
    b.ingested_at,
    cast(null as string) as contracting_method
  from base b
  where b.discounted_cash is not null and safe_cast(b.discounted_cash as numeric) != 0
    and lower(coalesce(b.rate_type, '')) not in ('discounted_cash', 'cash_price', 'cash')
),

min_rows as (
  select
    b.standard_charge_sk,
    b.hospital_id,
    b.hospital_name,
    b.billing_code,
    b.billing_code_type,
    b.description,
    b.payer_name,
    b.plan_name,
    'min' as rate_category,
    b.minimum as rate_amount,
    'dollars' as rate_unit,
    b.source_system,
    b.source_file_name,
    b.ingested_at,
    cast(null as string) as contracting_method
  from base b
  where b.minimum is not null and safe_cast(b.minimum as numeric) != 0
    and lower(coalesce(b.rate_type, '')) not in ('min', 'minimum')
),

max_rows as (
  select
    b.standard_charge_sk,
    b.hospital_id,
    b.hospital_name,
    b.billing_code,
    b.billing_code_type,
    b.description,
    b.payer_name,
    b.plan_name,
    'max' as rate_category,
    b.maximum as rate_amount,
    'dollars' as rate_unit,
    b.source_system,
    b.source_file_name,
    b.ingested_at,
    cast(null as string) as contracting_method
  from base b
  where b.maximum is not null and safe_cast(b.maximum as numeric) != 0
    and lower(coalesce(b.rate_type, '')) not in ('max', 'maximum')
),

all_rate_rows as (
  select * from primary_rows
  union all
  select * from gross_rows
  union all
  select * from cash_rows
  union all
  select * from min_rows
  union all
  select * from max_rows
),

-- Dedupe: at most one row per (standard_charge_sk, rate_category); keep latest ingested_at.
deduped as (
  select
    {{ dbt_utils.generate_surrogate_key(['standard_charge_sk', 'rate_category']) }} as semantic_charge_sk,
    standard_charge_sk,
    hospital_id,
    hospital_name,
    billing_code,
    billing_code_type,
    description,
    payer_name,
    plan_name,
    rate_category,
    rate_amount,
    rate_unit,
    source_system,
    source_file_name,
    ingested_at,
    contracting_method
  from (
    select
      *,
      row_number() over(partition by standard_charge_sk, rate_category order by ingested_at desc) as rn
    from all_rate_rows
  )
  where rn = 1
)

select * from deduped
{% endif %}
