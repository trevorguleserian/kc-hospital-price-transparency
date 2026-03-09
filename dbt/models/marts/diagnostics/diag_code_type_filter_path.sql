{{
  config(
    materialized='view',
    schema='marts',
    tags=['diagnostic'],
  )
}}

/*
  Billing code audit: whether any code types disappear between layers.
  Returns layer_name, billing_code_type, row_count, distinct_code_count.
  Layers: fct_standard_charges_semantic, fct_rates_comparable, fct_rates_comparable_harmonized, agg_hospital_procedure_compare.
  Run: dbt run --select diag_code_type_filter_path
*/
with semantic as (
  select
    'fct_standard_charges_semantic' as layer_name,
    coalesce(upper(replace(trim(cast(billing_code_type as string)), '-', '_')), 'UNKNOWN') as billing_code_type,
    count(*) as row_count,
    count(distinct trim(cast(billing_code as string))) as distinct_code_count
  from {{ ref('fct_standard_charges_semantic') }}
  where billing_code is not null
  group by 1, 2
),

comparable as (
  select
    'fct_rates_comparable' as layer_name,
    coalesce(upper(replace(trim(cast(billing_code_type as string)), '-', '_')), 'UNKNOWN') as billing_code_type,
    count(*) as row_count,
    count(distinct trim(cast(billing_code as string))) as distinct_code_count
  from {{ ref('fct_rates_comparable') }}
  where billing_code is not null
  group by 1, 2
),

harmonized as (
  select
    'fct_rates_comparable_harmonized' as layer_name,
    coalesce(upper(replace(trim(cast(billing_code_type as string)), '-', '_')), 'UNKNOWN') as billing_code_type,
    count(*) as row_count,
    count(distinct trim(cast(billing_code as string))) as distinct_code_count
  from {{ ref('fct_rates_comparable_harmonized') }}
  where billing_code is not null
  group by 1, 2
),

agg as (
  select
    'agg_hospital_procedure_compare' as layer_name,
    coalesce(upper(replace(trim(cast(billing_code_type as string)), '-', '_')), 'UNKNOWN') as billing_code_type,
    count(*) as row_count,
    count(distinct trim(cast(billing_code as string))) as distinct_code_count
  from {{ ref('agg_hospital_procedure_compare') }}
  where billing_code is not null
  group by 1, 2
)

select * from semantic
union all
select * from comparable
union all
select * from harmonized
union all
select * from agg
order by layer_name, row_count desc
