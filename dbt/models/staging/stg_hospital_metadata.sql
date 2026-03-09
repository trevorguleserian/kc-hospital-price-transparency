{{
  config(
    materialized='view',
    schema='staging',
  )
}}

/*
  One row per source_file_name with best-available hospital_name_clean for display.
  Fallback order:
  1) CSV: pt_csv_registry.preamble_kv (row 1 labels -> row 2 values, or row 1 key-value pairs).
       Try keys: hospital_name, hospital, facility_name, Hospital Name, Facility Name (case-sensitive in JSON).
  2) JSON: pt_json_registry has no preamble; use derived name from source_file_name.
  3) Derive from source_file_name: strip extension, _standardcharges, leading digits/underscore; replace _/- with space; initcap.
  4) Last resort: source_file_name as-is.
*/

with csv_clean as (
  select
    source_file_name,
    coalesce(
      nullif(trim(regexp_extract(to_json_string(preamble_kv), r'"hospital_name"\s*:\s*"([^"]+)"')), ''),
      nullif(trim(regexp_extract(to_json_string(preamble_kv), r'"hospital"\s*:\s*"([^"]+)"')), ''),
      nullif(trim(regexp_extract(to_json_string(preamble_kv), r'"facility_name"\s*:\s*"([^"]+)"')), ''),
      nullif(trim(regexp_extract(to_json_string(preamble_kv), r'"Hospital Name"\s*:\s*"([^"]+)"')), ''),
      nullif(trim(regexp_extract(to_json_string(preamble_kv), r'"Facility Name"\s*:\s*"([^"]+)"')), '')
    ) as hospital_name_clean
  from {{ source('pt_analytics', 'pt_csv_registry') }}
),

csv_derived as (
  select
    source_file_name,
    coalesce(
      nullif(trim(hospital_name_clean), ''),
      initcap(
        trim(
          regexp_replace(
            regexp_replace(
              regexp_replace(
                regexp_replace(lower(coalesce(source_file_name, '')), r'\.(csv|json)$', ''),
                r'_standardcharges$', ''
              ),
              r'^\d+_', ''
            ),
            r'[_\-\s]+', ' '
          )
        )
      ),
      source_file_name
    ) as hospital_name_clean
  from csv_clean
),

json_derived as (
  select
    source_file_name,
    initcap(
      trim(
        regexp_replace(
          regexp_replace(
            regexp_replace(
              regexp_replace(lower(coalesce(source_file_name, '')), r'\.(json|csv)$', ''),
              r'_standardcharges$', ''
            ),
            r'^\d+_', ''
          ),
          r'[_\-\s]+', ' '
        )
      )
    ) as hospital_name_clean
  from {{ source('pt_analytics', 'pt_json_registry') }}
),

unioned as (
  select source_file_name, hospital_name_clean from csv_derived
  union all
  select source_file_name, hospital_name_clean from json_derived
)

select
  source_file_name,
  coalesce(nullif(trim(hospital_name_clean), ''), source_file_name) as hospital_name_clean
from unioned
