{{
  config(
    materialized='view',
    schema='marts',
    tags=['diagnostic'],
  )
}}

/*
  Diagnostic: profile rate_category, rate_unit, billing_code_type from semantic fact
  to tune comparability allowlists and understand unit presence.
*/
with base as (
  select
    lower(trim(cast(rate_category as string))) as rate_category_norm,
    lower(trim(cast(rate_unit as string))) as rate_unit_norm,
    lower(trim(cast(billing_code_type as string))) as billing_code_type_norm,
    rate_amount,
    rate_unit
  from {{ ref('fct_standard_charges_semantic') }}
),
agg as (
  select
    rate_category_norm,
    rate_unit_norm,
    billing_code_type_norm,
    count(*) as row_count,
    countif(safe_cast(rate_amount as numeric) is not null) as rows_with_numeric_rate,
    countif(trim(cast(rate_unit as string)) != '') as rows_with_unit
  from base
  group by 1, 2, 3
)
select * from agg
order by row_count desc
limit 500
