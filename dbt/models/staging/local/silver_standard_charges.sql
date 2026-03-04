{{
  config(
    materialized='view',
    schema='staging',
    tags=['local'],
  )
}}

-- Local mode only: pass-through view over Silver Parquet. Schema includes payer_name, plan_name when present (union_by_name).
select
  source_file_name,
  source_format,
  ingest_date,
  billing_code,
  description,
  rate_type,
  rate_amount,
  ingested_at,
  payer_name,
  plan_name
from read_parquet('{{ var("silver_parquet_glob") }}', union_by_name = true)
