{{
  config(
    materialized='table',
    schema='marts',
    tags=['local'],
  )
}}

/*
  Payer/plan comparison: same grain as agg_hospital_procedure_compare. Selects only from agg_hospital_procedure_compare,
  so rate_category='other' is excluded by upstream; no extra filter needed here.
  Used by Streamlit Payer/Plan Comparison page to aggregate by payer_family or plan_family.
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
