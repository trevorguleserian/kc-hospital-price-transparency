{{
  config(
    materialized='view',
    schema='marts',
    tags=['diagnostic'],
  )
}}

/*
  Diagnostic: confirm whether rate_category='other' leak is due to casing/spacing variants.
  A. Upstream (harmonized): distinct raw rate_category values that normalize to 'other', with is_comparable and counts.
  B. Agg output: distinct rate_category values in agg that normalize to 'other' (should be empty after fix).
  Run: dbt run --select diag_other_rate_category_variants, then query this view.
*/
with harmonized_variants as (
  select
    f.rate_category as raw_rate_category,
    lower(trim(cast(f.rate_category as string))) as rate_category_norm,
    f.is_comparable,
    count(*) as n
  from {{ ref('fct_rates_comparable_harmonized') }} f
  where lower(trim(cast(f.rate_category as string))) = 'other'
  group by 1, 2, 3
),
agg_variants as (
  select
    rate_category,
    lower(trim(cast(rate_category as string))) as norm,
    count(*) as n
  from {{ ref('agg_hospital_procedure_compare') }}
  where lower(trim(cast(rate_category as string))) = 'other'
  group by 1, 2
)
select
  'harmonized' as source,
  cast(raw_rate_category as string) as raw_or_category,
  rate_category_norm as rate_category_norm,
  is_comparable,
  n
from harmonized_variants
union all
select
  'agg' as source,
  cast(rate_category as string) as raw_or_category,
  norm as rate_category_norm,
  cast(null as bool) as is_comparable,
  n
from agg_variants
order by source, n desc
