{{
  config(
    materialized='view',
    schema='marts',
    tags=['local'],
  )
}}

/*
  Rows where is_comparable = FALSE (e.g. rate_category = 'other', category_not_allowed,
  missing_rate_unit). Limited to most recent 1M by ingested_at for diagnostics.
*/
with ranked as (
  select
    *,
    row_number() over (order by ingested_at desc nulls last) as rn
  from {{ ref('fct_rates_comparable') }}
  where is_comparable = false
)
select
  semantic_charge_sk,
  standard_charge_sk,
  hospital_id,
  hospital_name,
  billing_code,
  billing_code_type,
  description,
  payer_name,
  plan_name,
  rate_category,
  rate_amount,
  rate_unit,
  comparability_reason,
  source_system,
  source_file_name,
  ingested_at,
  contracting_method
from ranked
where rn <= 1000000
