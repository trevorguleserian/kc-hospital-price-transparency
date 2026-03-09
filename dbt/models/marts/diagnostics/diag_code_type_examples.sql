{{
  config(
    materialized='view',
    schema='marts',
    tags=['diagnostic'],
  )
}}

/*
  Billing code audit: sample examples per code type (top 50 most frequent codes per type).
  Returns billing_code_type, billing_code, description, row_count.
  Source: fct_standard_charges_semantic.
*/
with grouped as (
  select
    coalesce(upper(replace(trim(cast(billing_code_type as string)), '-', '_')), 'UNKNOWN') as billing_code_type,
    trim(cast(billing_code as string)) as billing_code,
    trim(coalesce(cast(description as string), '')) as description,
    count(*) as row_count
  from {{ ref('fct_standard_charges_semantic') }}
  where billing_code is not null
  group by 1, 2, 3
),

ranked as (
  select
    billing_code_type,
    billing_code,
    description,
    row_count,
    row_number() over (partition by billing_code_type order by row_count desc) as rn
  from grouped
)

select billing_code_type, billing_code, description, row_count
from ranked
where rn <= 50
order by billing_code_type, row_count desc
