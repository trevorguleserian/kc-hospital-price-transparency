{{
  config(
    materialized='table',
    schema='marts',
    tags=['local'],
  )
}}

/*
  Compiled SQL check: dbt/target/run/.../agg_hospital_procedure_compare.sql must contain
  "where coalesce(is_comparable, false) = true and lower(trim(cast(rate_category as string))) != 'other'".
  If the table in BQ still had other, the target was from an older run; full-refresh fixes it.

  Grain: (billing_code, billing_code_type, payer_family, plan_family, rate_category, rate_unit, comparability_key, hospital_id)
  One row per hospital per procedure/payer/plan/rate-type combination; metrics support hospital-to-hospital comparison.
  Source: fct_rates_comparable_harmonized. Contract: only comparable rows; rate_category='other' must never appear.
  We normalize rate_category (lower/trim) and hard-exclude other so casing/spacing cannot leak through.
  coalesce(is_comparable, false) guards against nulls.
*/

with base as (
  select
    billing_code,
    billing_code_type,
    payer_family,
    plan_family,
    lower(trim(cast(rate_category as string))) as rate_category,
    rate_unit,
    comparability_key,
    hospital_id,
    rate_amount,
    ingested_at
  from {{ ref('fct_rates_comparable_harmonized') }}
  where coalesce(is_comparable, false) = true
    and lower(trim(cast(rate_category as string))) != 'other'
),

agg as (
  select
    billing_code,
    billing_code_type,
    payer_family,
    plan_family,
    rate_category,
    rate_unit,
    comparability_key,
    hospital_id,
    min(rate_amount) as min_rate,
    max(rate_amount) as max_rate,
    {% if var('execution_mode', 'bq') == 'local' %}
    median(rate_amount) as approx_median_rate,
    {% else %}
    approx_quantiles(rate_amount, 100)[offset(50)] as approx_median_rate,
    {% endif %}
    count(*) as row_count,
    max(ingested_at) as last_ingested_at
  from base
  group by
    billing_code,
    billing_code_type,
    payer_family,
    plan_family,
    rate_category,
    rate_unit,
    comparability_key,
    hospital_id
)

select
  billing_code,
  billing_code_type,
  payer_family,
  plan_family,
  rate_category,
  rate_unit,
  comparability_key,
  hospital_id,
  min_rate,
  max_rate,
  approx_median_rate,
  row_count,
  last_ingested_at
from agg
