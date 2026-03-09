/*
  Singular test: fail when hospital_name_clean still begins with CCN/NPI-style numeric prefix
  (1-3 digits, space(s), 5+ digits) that should have been stripped in dim_hospital.
  Does not fail on names that merely start with a digit (e.g. "123 Main Street").
*/
select
  hospital_id,
  hospital_name,
  hospital_name_clean,
  source_file_name
from {{ ref('dim_hospital') }}
where regexp_contains(trim(coalesce(hospital_name_clean, '')), r'^\d{1,3}\s+\d{5,}')
