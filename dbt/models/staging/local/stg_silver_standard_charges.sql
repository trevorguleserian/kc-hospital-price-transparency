{{
  config(
    materialized='view',
    schema='staging',
    tags=['local'],
  )
}}

-- Local mode: canonical shape from Silver Parquet; derive hospital fields from source_file_name (no hospital_name in Silver).
with base as (
  select * from {{ ref('silver_standard_charges') }}
),

parsed as (
  select
    cast(base.source_file_name as string) as source_file_name,
    base.ingested_at,
    base.rate_type,
    base.rate_amount,
    cast(base.description as string) as description,
    cast(base.billing_code as string) as billing_code,
    cast(base.payer_name as string) as payer_name,
    cast(base.plan_name as string) as plan_name,
    coalesce(
      cast(base.source_format as string),
      case when lower(coalesce(cast(base.source_file_name as string), '')) like '%.json%' then 'json' when lower(coalesce(cast(base.source_file_name as string), '')) like '%.csv%' then 'csv' else null end
    ) as source_format,
    regexp_replace(base.source_file_name, '^.*[\\\\/]', '') as filename_only,
    regexp_replace(regexp_replace(base.source_file_name, '^.*[\\\\/]', ''), '\\.(csv|json|parquet)$', '') as filename_no_ext
  from base
),

hosp as (
  select
    parsed.*,
    regexp_replace(parsed.filename_no_ext, '^\\d+_', '') as filename_no_id,
    regexp_replace(regexp_replace(parsed.filename_no_ext, '^\\d+_', ''), '_standardcharges.*$', '') as hospital_token,
    trim(regexp_replace(regexp_replace(regexp_replace(parsed.filename_no_ext, '^\\d+_', ''), '_standardcharges.*$', ''), '_+', ' ')) as hospital_name
  from parsed
),

final as (
  select
    hosp.*,
    lower(coalesce(nullif(trim(hosp.hospital_name), ''), 'unknown')) as hospital_name_clean,
    regexp_replace(lower(coalesce(nullif(trim(hosp.hospital_name), ''), 'unknown')), '[^a-z0-9]+', ' ') as hospital_name_norm,
    md5(lower(coalesce(nullif(trim(hosp.hospital_name), ''), 'unknown'))) as hospital_id,
    -- source_system: only json | csv_wide | csv_tall (accepted_values test). Local: map source_format -> json or csv_wide; never 'other'.
    case
      when lower(coalesce(hosp.source_format, '')) like '%json%' then 'json'
      when lower(coalesce(hosp.source_format, '')) like '%csv%'  then 'csv_wide'
      else 'csv_wide'
    end as source_system,
    {{ classify_billing_code_type('hosp.billing_code') }} as billing_code_type
    {% if var('execution_mode') == 'local' %}
    , coalesce(hosp.payer_name, cast(null as string)) as payer_name
    , coalesce(hosp.plan_name, cast(null as string)) as plan_name
    , cast(null as string) as raw_rate_json
    {% endif %}
  from hosp
),

with_sk as (
  select
    {% if var('execution_mode') == 'local' %}
    {{ dbt_utils.generate_surrogate_key([
      'source_system',
      'source_file_name',
      'ingested_at',
      'hospital_id',
      'billing_code',
      'rate_type',
      'cast(rate_amount as string)'
    ]) }} as standard_charge_sk,
    {% else %}
    {{ dbt_utils.generate_surrogate_key([
      'source_system',
      'source_file_name',
      'ingested_at',
      'hospital_id',
      'billing_code',
      'billing_code_type',
      'payer_name',
      'plan_name',
      'rate_type',
      'cast(rate_amount as string)',
      'raw_rate_json'
    ]) }} as standard_charge_sk,
    {% endif %}
    final.*
  from final
)

select * from with_sk
