{{
  config(
    materialized='table',
    schema='marts',
  )
}}

/*
  One row per (source_file_name, source_system). Joins to pt_csv_registry and pt_json_registry for metadata.
  CSV: delimiter, encoding, header_count, preamble-derived hospital_name, rows_loaded, status.
  JSON: rows_loaded, rows_extracted, status.
*/
with csv_registry as (
  select
    source_file_name,
    case when lower(trim(coalesce(format_hint, ''))) = 'wide' then 'csv_wide' else 'csv_tall' end as source_system,
    ingested_at,
    format_hint,
    header_count,
    delimiter,
    encoding,
    rows_loaded,
    status,
    coalesce(
      nullif(trim(safe_cast(json_value(preamble_kv, '$.hospital_name') as string)), ''),
      nullif(trim(safe_cast(json_value(preamble_kv, '$.hospital') as string)), ''),
      nullif(trim(safe_cast(json_value(preamble_kv, '$.facility_name') as string)), ''),
      source_file_name
    ) as hospital_name
  from {{ source('pt_analytics', 'pt_csv_registry') }}
),

json_registry as (
  select
    source_file_name,
    'json' as source_system,
    ingested_at,
    cast(null as string) as format_hint,
    cast(null as int64) as header_count,
    cast(null as string) as delimiter,
    cast(null as string) as encoding,
    rows_loaded,
    status,
    source_file_name as hospital_name
  from {{ source('pt_analytics', 'pt_json_registry') }}
),

unioned as (
  select
    source_file_name,
    source_system,
    hospital_name,
    ingested_at as ingested_at_min,
    ingested_at as ingested_at_max,
    format_hint,
    header_count,
    delimiter,
    encoding,
    rows_loaded,
    status
  from csv_registry
  union all
  select
    source_file_name,
    source_system,
    hospital_name,
    ingested_at,
    ingested_at,
    format_hint,
    header_count,
    delimiter,
    encoding,
    rows_loaded,
    status
  from json_registry
),

-- One row per (source_file_name, source_system); aggregate min/max if same file appears multiple times.
deduped as (
  select
    source_file_name,
    source_system,
    any_value(hospital_name) as hospital_name,
    min(ingested_at_min) as ingested_at_min,
    max(ingested_at_max) as ingested_at_max,
    any_value(format_hint) as format_hint,
    any_value(header_count) as header_count,
    any_value(delimiter) as delimiter,
    any_value(encoding) as encoding,
    any_value(rows_loaded) as rows_loaded,
    any_value(status) as status
  from unioned
  group by source_file_name, source_system
)

select * from deduped
