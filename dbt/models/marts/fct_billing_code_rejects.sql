{{
  config(
    materialized='table',
    schema='marts',
    tags=['diagnostic'],
  )
}}

/*
  Reject/diagnostic mart: invalid billing codes from the comparable pipeline.
  One row per (source_layer, billing_code_raw, billing_code_type, billing_code_issue_reason) with row_count.
  Invalid codes are not dropped silently; they are available here and in fct_rates_comparable with billing_code_is_valid = false.
  App-facing aggs (agg_hospital_procedure_compare, agg_payer_plan_compare) exclude these; use this model for audit.
*/
select
  'fct_rates_comparable' as source_layer,
  billing_code_raw,
  billing_code_type,
  billing_code_issue_reason,
  count(*) as row_count
from {{ ref('fct_rates_comparable') }}
where coalesce(billing_code_is_valid, false) = false
  and billing_code_issue_reason is not null
group by 1, 2, 3, 4
order by row_count desc
