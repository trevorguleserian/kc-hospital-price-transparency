{{
  config(
    materialized='view',
    schema='marts',
    tags=['diagnostic'],
  )
}}

/*
  Diagnostic: rate_category (normalized lower/trim) and source_system counts from semantic.
  Use to see why agg might be empty (e.g. all 'other') and to verify mapping fixes.
  Run: dbt run --select diag_rate_category_counts, then query this view.
*/
select
  lower(trim(cast(rate_category as string))) as rate_category,
  source_system,
  count(*) as rows_,
  count(distinct hospital_id) as hospitals
from {{ ref('fct_standard_charges_semantic') }}
group by 1, 2
order by rows_ desc
