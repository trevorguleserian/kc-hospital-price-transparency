{{
  config(
    materialized='view',
    schema='marts',
    tags=['diagnostic'],
  )
}}

/*
  Diagnostic: billing_code_type values in dim_procedure that are NOT in the accepted_values allowlist.
  dim_procedure standardizes to upper + underscores; this list must match schema.yml (deduped).
  Run: dbt run --select diag_dim_procedure_billing_code_type_unexpected, then query this view.
*/
select
  billing_code_type,
  count(*) as n
from {{ ref('dim_procedure') }}
group by 1
having billing_code_type not in ('CPT','HCPCS','NDC','REVENUE','ICD_10_PCS','UNKNOWN','DRG','MS_DRG','APC','EAPG','ICD_10_CM','RC','CDM','8','APR_DRG','ICD','TRIS_DRG','HIPPS')
order by n desc
