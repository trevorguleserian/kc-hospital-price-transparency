{{
  config(
    materialized='view',
    schema='marts',
    tags=['diagnostic'],
  )
}}

/*
  Diagnostic: sample of semantic rows with raw source fields used to derive rate_category.
  Exposes rate_type (and typed amount columns) from base so we can see why rows become 'other'.
  Run: dbt run --select diag_semantic_rate_category_source_fields, then query this view.
*/
with base as (
  select
    standard_charge_sk,
    hospital_id,
    hospital_name,
    billing_code,
    billing_code_type,
    rate_type,
    rate_amount,
    gross_charge,
    discounted_cash,
    minimum,
    maximum,
    source_system,
    source_file_name,
    ingested_at
  from {{ ref('int_standard_charges_base') }}
  where billing_code is not null
),
semantic as (
  select
    standard_charge_sk,
    rate_category,
    rate_amount as semantic_rate_amount,
    rate_unit
  from {{ ref('fct_standard_charges_semantic') }}
)
select
  b.source_system,
  b.source_file_name,
  b.hospital_id,
  b.billing_code,
  b.standard_charge_sk,
  b.rate_type                                      as raw_rate_type,
  lower(trim(coalesce(cast(b.rate_type as string), ''))) as rate_type_norm,
  b.rate_amount                                    as raw_rate_amount,
  b.gross_charge,
  b.discounted_cash,
  b.minimum,
  b.maximum,
  s.rate_category                                  as current_rate_category,
  s.semantic_rate_amount,
  s.rate_unit
from base b
join semantic s on b.standard_charge_sk = s.standard_charge_sk
order by b.source_system, b.standard_charge_sk, s.rate_category
limit 5000
