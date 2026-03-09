{{
  config(
    materialized='view',
    schema='marts',
    tags=['diagnostic'],
  )
}}

/*
  Billing code audit: inventory of every billing_code_type in the semantic fact.
  Returns row_count, distinct_code_count, distinct_description_count per type.
  Source: fct_standard_charges_semantic.
  Run: dbt run --select diag_code_type_inventory
*/
select
  coalesce(upper(replace(trim(cast(billing_code_type as string)), '-', '_')), 'UNKNOWN') as billing_code_type,
  count(*) as row_count,
  count(distinct trim(cast(billing_code as string))) as distinct_code_count,
  count(distinct trim(coalesce(cast(description as string), ''))) as distinct_description_count
from {{ ref('fct_standard_charges_semantic') }}
where billing_code is not null
group by 1
order by row_count desc
