{{
  config(
    materialized='view',
    schema='marts',
    tags=['diagnostic'],
  )
}}

/*
  Billing code audit: rows violating expected format rules by code type.
  Returns billing_code_type, billing_code, issue_type, row_count.
  Uses centralized billing_code_issue_reason macro.
  Source: fct_standard_charges_semantic.
*/
with base as (
  select
    trim(cast(billing_code as string)) as billing_code,
    coalesce(upper(replace(trim(cast(billing_code_type as string)), '-', '_')), 'UNKNOWN') as billing_code_type
  from {{ ref('fct_standard_charges_semantic') }}
  where billing_code is not null and billing_code_type is not null
),

with_reason as (
  select
    billing_code,
    billing_code_type,
    {{ billing_code_issue_reason('billing_code', 'billing_code_type') }} as issue_type
  from base
)

select
  billing_code_type,
  billing_code,
  issue_type,
  count(*) as row_count
from with_reason
where issue_type is not null
group by 1, 2, 3
order by row_count desc
