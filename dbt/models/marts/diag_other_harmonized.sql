{{
  config(
    materialized='view',
    schema='marts',
    tags=['diagnostic'],
  )
}}

/*
  A. Harmonized fact: breakdown of rate_category='other' (normalized) by is_comparable and comparability_reason.
     If any row has is_comparable = TRUE, fix upstream so other cannot be comparable.

  B. Run in BigQuery to check if agg filter would see any comparable 'other' (should be 0):
     select count(*) as n
     from `pricing-transparency-portfolio.pt_analytics_marts.fct_rates_comparable_harmonized`
     where is_comparable = true
       and lower(trim(cast(rate_category as string))) = 'other';
     If n > 0: fix upstream. If n = 0 but agg still has other: compiled SQL / schema mismatch.
*/
select
  lower(trim(cast(rate_category as string))) as rate_category_norm,
  is_comparable,
  comparability_reason,
  count(*) as n
from {{ ref('fct_rates_comparable_harmonized') }}
where lower(trim(cast(rate_category as string))) = 'other'
group by 1, 2, 3
order by n desc
