{{
  config(
    materialized='table',
    schema='marts',
    tags=['local'],
  )
}}

with unioned as (
  select distinct billing_code, billing_code_type, description
  from {{ ref('int_standard_charges_base') }}
  where billing_code is not null or description is not null
)

select
  coalesce(billing_code, '') as billing_code,
  {{ infer_billing_code_type_if_unknown('billing_code', 'billing_code_type') }} as billing_code_type,
  description
from unioned
