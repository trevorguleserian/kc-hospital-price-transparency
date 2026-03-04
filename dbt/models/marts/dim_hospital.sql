{{
  config(
    materialized='table',
    schema='marts',
    tags=['local'],
  )
}}

{% if var('execution_mode', 'bq') == 'local' %}
/*
  Local mode: one row per hospital_id from int_standard_charges_base (DuckDB-compatible).
*/
select
  cast(hospital_id as string) as hospital_id,
  cast(any_value(coalesce(hospital_name, hospital_name_clean)) as string) as hospital_name,
  cast(any_value(hospital_name_clean) as string) as hospital_name_clean,
  cast(any_value(hospital_name_norm) as string) as hospital_name_norm,
  cast(any_value(source_system) as string) as source_system,
  cast(any_value(source_file_name) as string) as source_file_name
from {{ ref('int_standard_charges_base') }}
group by hospital_id
{% else %}
/*
  Full coverage: one row per source_file_name present in the union.
  Enrich hospital names from CSV/JSON registries when available; else derive from source_file_name; last resort use source_file_name.
  Ensures every fact row can join to dim_hospital on source_file_name so relationships pass.
*/
with base as (
  select distinct
    source_file_name,
    any_value(source_system) as source_system
  from {{ ref('int_standard_charges_base') }}
  group by source_file_name
),

csv_registry as (
  select
    source_file_name,
    coalesce(
      nullif(trim(safe_cast(json_value(preamble_kv, '$.hospital_name') as string)), ''),
      nullif(trim(safe_cast(json_value(preamble_kv, '$.hospital') as string)), ''),
      nullif(trim(safe_cast(json_value(preamble_kv, '$.facility_name') as string)), '')
    ) as preamble_name
  from {{ source('pt_analytics', 'pt_csv_registry') }}
),

enriched as (
  select
    b.source_file_name,
    cast(b.source_system as string) as source_system,
    r.preamble_name
  from base b
  left join csv_registry r on b.source_file_name = r.source_file_name
),

-- Derive display name from source_file_name: strip extension, _standardcharges, leading digits/underscore; replace _/- with space; collapse spaces; initcap.
derived_from_file as (
  select
    source_file_name,
    source_system,
    preamble_name,
    initcap(
      trim(
        (
          regexp_replace(
            regexp_replace(
              regexp_replace(
                regexp_replace(
                  regexp_replace(lower(coalesce(source_file_name, '')), r'\.(json|csv)$', ''),
                  r'_standardcharges$', ''
                ),
                r'^\d+_', ''
              ),
              r'[_\-\s]+', ' '
            ),
            r' +', ' '
          )
        )
      )
    ) as derived_name
  from enriched
),

-- hospital_name_clean: prefer preamble, else derived from file, else source_file_name (last resort). Then collapse spaces.
with_clean as (
  select
    source_file_name,
    source_system,
    coalesce(
      nullif(trim(preamble_name), ''),
      nullif(trim(derived_name), ''),
      source_file_name
    ) as hospital_name_raw,
    trim(
      (
        regexp_replace(
          regexp_replace(
            coalesce(nullif(trim(preamble_name), ''), nullif(trim(derived_name), ''), source_file_name),
            r'[_\-\s]+', ' '
          ),
          r' +', ' '
        )
      )
    ) as hospital_name_clean
  from derived_from_file
),

with_clean_final as (
  select
    w.source_file_name,
    w.source_system,
    cast(w.hospital_name_raw as string) as hospital_name,
    coalesce(nullif(trim(w.hospital_name_clean), ''), w.source_file_name) as hospital_name_clean
  from with_clean w
),

-- hospital_name_norm: lower, remove punctuation, collapse spaces.
with_norm as (
  select
    source_file_name,
    source_system,
    hospital_name,
    hospital_name_clean,
    trim(
      (
        regexp_replace(
          regexp_replace(lower(hospital_name_clean), r'[^a-z0-9\s]', ''),
          r' +', ' '
        )
      )
    ) as hospital_name_norm
  from with_clean_final
),

-- Ensure norm is never empty (use clean or source_file_name).
with_norm_safe as (
  select
    source_file_name,
    source_system,
    hospital_name,
    hospital_name_clean,
    coalesce(nullif(trim(hospital_name_norm), ''), lower(trim(hospital_name_clean)), lower(trim(source_file_name))) as hospital_name_norm
  from with_norm
),

with_id as (
  select
    {{ dbt_utils.generate_surrogate_key(['hospital_name_norm']) }} as hospital_id,
    hospital_name,
    hospital_name_clean,
    hospital_name_norm,
    source_system,
    source_file_name
  from with_norm_safe
)

-- One row per source_file_name; all columns STRING.
select
  cast(hospital_id as string) as hospital_id,
  cast(hospital_name as string) as hospital_name,
  cast(hospital_name_clean as string) as hospital_name_clean,
  cast(hospital_name_norm as string) as hospital_name_norm,
  cast(source_system as string) as source_system,
  cast(source_file_name as string) as source_file_name
from with_id
{% endif %}
