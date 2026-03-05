{{
  config(
    materialized='view',
    schema='marts',
    tags=['diagnostic'],
  )
}}

/*
  One-off diagnostic: rate_category values in fct_rates_comparable that fall outside
  the schema accepted_values allowlist. Run "dbt run --select diag_fct_rates_comparable_rate_category"
  then query this view to see offending rate_category_norm and row_count.
*/
select
  lower(trim(cast(rate_category as string))) as rate_category_norm,
  count(*) as row_count
from {{ ref('fct_rates_comparable') }}
group by 1
having rate_category_norm not in ('negotiated', 'gross', 'cash', 'min', 'max', 'percentage')
order by row_count desc
