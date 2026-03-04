-- =============================================================================
-- Validation queries for raw landing tables (pt_json_raw, pt_csv_raw)
-- Run in BigQuery console against dataset pt_analytics after running load scripts.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1) Counts by source_file_name (both tables)
-- -----------------------------------------------------------------------------
SELECT
  'pt_json_raw' AS table_name,
  source_file_name,
  COUNT(*) AS row_count,
  MIN(ingested_at) AS first_ingested,
  MAX(ingested_at) AS last_ingested
FROM pt_analytics.pt_json_raw
GROUP BY source_file_name
ORDER BY source_file_name;

SELECT
  'pt_csv_raw' AS table_name,
  source_file_name,
  COUNT(*) AS row_count,
  MIN(ingested_at) AS first_ingested,
  MAX(ingested_at) AS last_ingested
FROM pt_analytics.pt_csv_raw
GROUP BY source_file_name
ORDER BY source_file_name;

-- -----------------------------------------------------------------------------
-- 2) Sample JSON keys from pt_json_raw (first row)
-- -----------------------------------------------------------------------------
SELECT
  source_file_name,
  ingested_at,
  (SELECT ARRAY_AGG(key ORDER BY key) FROM UNNEST(JSON_KEYS(raw)) AS key) AS sample_json_keys
FROM pt_analytics.pt_json_raw
LIMIT 1;

-- -----------------------------------------------------------------------------
-- 3) Sample 5 rows from pt_csv_raw per source file
-- -----------------------------------------------------------------------------
SELECT *
FROM (
  SELECT
    source_file_name,
    ingested_at,
    raw,
    ROW_NUMBER() OVER (PARTITION BY source_file_name ORDER BY ingested_at) AS rn
  FROM pt_analytics.pt_csv_raw
)
WHERE rn <= 5
ORDER BY source_file_name, rn;

-- -----------------------------------------------------------------------------
-- 4) Counts grouped by _meta_format_hint (pt_csv_raw)
-- -----------------------------------------------------------------------------
SELECT
  JSON_VALUE(raw, '$._meta_format_hint') AS format_hint,
  COUNT(*) AS row_count
FROM pt_analytics.pt_csv_raw
GROUP BY JSON_VALUE(raw, '$._meta_format_hint')
ORDER BY format_hint;

-- -----------------------------------------------------------------------------
-- 5) Confirm JSON typing (raw column is JSON type, not string)
-- -----------------------------------------------------------------------------
SELECT COUNTIF(JSON_TYPE(raw) IS NOT NULL) AS json_typed_rows FROM pt_analytics.pt_json_raw;

SELECT COUNTIF(JSON_TYPE(raw) IS NOT NULL) AS json_typed_rows FROM pt_analytics.pt_csv_raw;

-- -----------------------------------------------------------------------------
-- 6) pt_json_registry: counts by source_file_name (even if empty initially)
-- -----------------------------------------------------------------------------
SELECT
  source_file_name,
  COUNT(*) AS row_count,
  MIN(ingested_at) AS first_ingested,
  MAX(ingested_at) AS last_ingested
FROM pt_analytics.pt_json_registry
GROUP BY source_file_name
ORDER BY source_file_name;

-- -----------------------------------------------------------------------------
-- 7) pt_json_extracted_rates: counts by source_file_name and record_path
-- -----------------------------------------------------------------------------
SELECT
  source_file_name,
  record_path,
  COUNT(*) AS row_count,
  MIN(ingested_at) AS first_ingested,
  MAX(ingested_at) AS last_ingested
FROM pt_analytics.pt_json_extracted_rates
GROUP BY source_file_name, record_path
ORDER BY source_file_name, record_path;

-- -----------------------------------------------------------------------------
-- 8) Sample 20 rows from pt_json_extracted_rates
-- -----------------------------------------------------------------------------
SELECT *
FROM pt_analytics.pt_json_extracted_rates
LIMIT 20;
