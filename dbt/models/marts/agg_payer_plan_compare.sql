{{
  config(
    materialized='table',
    schema='marts',
    tags=['local'],
  )
}}

/*
  Payer/plan comparison: same grain as agg_hospital_procedure_compare. Selects only from agg_hospital_procedure_compare
  (which already excludes rate_category='other' and invalid billing codes). Used by Streamlit Payer/Plan Comparison page.
*/
select
  billing_code,
  billing_code_type,
  payer_family,
  plan_family,
  rate_category,
  rate_unit,
  comparability_key,
  hospital_id,
  min_rate,
  max_rate,
  approx_median_rate,
  row_count,
  last_ingested_at
from {{ ref('agg_hospital_procedure_compare') }}
