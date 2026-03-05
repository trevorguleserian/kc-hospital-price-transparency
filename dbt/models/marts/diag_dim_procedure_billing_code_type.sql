{{
  config(
    materialized='view',
    schema='marts',
    tags=['diagnostic'],
  )
}}

/*
  Diagnostic: distinct billing_code_type values in dim_procedure with row counts.
  Use to find values that fail accepted_values so you can add them to schema.yml allowlist.
  Run: dbt run --select diag_dim_procedure_billing_code_type, then query this view.
*/
select
  billing_code_type,
  count(*) as row_count
from {{ ref('dim_procedure') }}
group by 1
order by row_count desc
