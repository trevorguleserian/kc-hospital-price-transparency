{{
  config(
    materialized='view',
    schema='marts',
    tags=['diagnostic'],
  )
}}

/*
  Diagnostic: rows where hospital_name_clean still appears to start with numeric prefixes
  (e.g. "45 0503121 Centerpoint" before strip, or remaining after strip). Use to verify cleanup.
  Run: dbt run --select diag_hospital_name_display_issues, then query this view.
*/
select
  hospital_id,
  hospital_name,
  hospital_name_clean,
  source_file_name
from {{ ref('dim_hospital') }}
where regexp_contains(trim(coalesce(hospital_name_clean, '')), r'^\d')
order by hospital_name_clean
