{{
  config(
    materialized='table',
    schema='marts',
    tags=['local'],
  )
}}

with unioned as (
  select distinct payer_name, plan_name
  from {{ ref('int_standard_charges_base') }}
  where payer_name is not null or plan_name is not null
)

select
  coalesce(payer_name, '') as payer_name,
  coalesce(plan_name, '') as plan_name
from unioned
