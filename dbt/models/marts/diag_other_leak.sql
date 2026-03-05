{{
  config(
    materialized='view',
    schema='marts',
    tags=['diagnostic'],
  )
}}

/*
  Diagnostic: locate leak of rate_category='other' into comparison marts.
  A: Is other ever marked comparable in fct_rates_comparable?
  B: Do other rows exist in fct_rates_comparable_harmonized (and with what is_comparable)?
  Run: dbt run --select diag_other_leak then query this view.
*/
with a as (
  select
    'fct_rates_comparable' as source,
    rate_category,
    is_comparable,
    comparability_reason,
    count(*) as n
  from {{ ref('fct_rates_comparable') }}
  where rate_category = 'other'
  group by 1, 2, 3, 4
),
b as (
  select
    'fct_rates_comparable_harmonized' as source,
    rate_category,
    is_comparable,
    comparability_reason,
    count(*) as n
  from {{ ref('fct_rates_comparable_harmonized') }}
  where rate_category = 'other'
  group by 1, 2, 3, 4
)
select * from a
union all
select * from b
order by source, n desc
