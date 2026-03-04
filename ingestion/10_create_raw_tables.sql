-- =============================================================================
-- Raw landing tables for hospital price transparency data (Phase 2)
-- =============================================================================
-- These tables form the landing layer in BigQuery. They are referenced by dbt
-- SOURCES (see dbt/models/sources/sources.yml) and are NOT transformed by dbt.
-- Ingestion (loads from GCS, external tables, or COPY) will be added later.
-- =============================================================================

-- Create dataset if it does not exist (idempotent; safe to re-run)
CREATE SCHEMA IF NOT EXISTS pt_analytics
  OPTIONS(
    description = 'Hospital price transparency analytics: raw landing and dbt models'
  );

-- -----------------------------------------------------------------------------
-- pt_json_raw: landing table for JSON-format price transparency files
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pt_analytics.pt_json_raw (
  source_file_name STRING NOT NULL OPTIONS(description = 'Original file name as ingested'),
  ingested_at      TIMESTAMP NOT NULL OPTIONS(description = 'Timestamp when the record was first ingested'),
  raw              JSON OPTIONS(description = 'Full JSON payload; structure may vary by source')
)
OPTIONS(
  description = 'Landing table for JSON price transparency files. Referenced by dbt source raw.pt_json_raw.'
);

-- -----------------------------------------------------------------------------
-- pt_csv_raw: landing table for CSV-format price transparency files (tall/wide)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pt_analytics.pt_csv_raw (
  source_file_name STRING NOT NULL OPTIONS(description = 'Original file name as ingested'),
  ingested_at      TIMESTAMP NOT NULL OPTIONS(description = 'Timestamp when the record was first ingested'),
  raw              JSON OPTIONS(description = 'CSV row represented as JSON object; keys are column names')
)
OPTIONS(
  description = 'Landing table for CSV price transparency files. Referenced by dbt source raw.pt_csv_raw.'
);

-- -----------------------------------------------------------------------------
-- pt_csv_raw_tall: TALL-format CSV rows only (one row per rate line)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pt_analytics.pt_csv_raw_tall (
  source_file_name STRING NOT NULL OPTIONS(description = 'Original file name as ingested'),
  ingested_at      TIMESTAMP NOT NULL OPTIONS(description = 'Timestamp when the record was first ingested'),
  raw              JSON OPTIONS(description = 'CSV row as JSON with _meta_format_hint, _meta_row_number, _meta_header_column_count')
)
OPTIONS(
  description = 'TALL CSV price transparency files only; loaded by GCS-first bulk runner.'
);

-- -----------------------------------------------------------------------------
-- pt_csv_raw_wide: WIDE-format CSV rows only (one row per procedure, many columns)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pt_analytics.pt_csv_raw_wide (
  source_file_name STRING NOT NULL OPTIONS(description = 'Original file name as ingested'),
  ingested_at      TIMESTAMP NOT NULL OPTIONS(description = 'Timestamp when the record was first ingested'),
  raw              JSON OPTIONS(description = 'CSV row as JSON with _meta_format_hint, _meta_row_number, _meta_header_column_count')
)
OPTIONS(
  description = 'WIDE CSV price transparency files only; loaded by GCS-first bulk runner.'
);

-- -----------------------------------------------------------------------------
-- pt_csv_registry: file-level registry for CSV PT files (preamble rows 1–2, no per-row duplication)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pt_analytics.pt_csv_registry (
  source_file_name STRING NOT NULL OPTIONS(description = 'Original file name'),
  run_id           STRING OPTIONS(description = 'Bulk run identifier'),
  ingested_at      TIMESTAMP NOT NULL OPTIONS(description = 'When the file was registered'),
  format_hint      STRING OPTIONS(description = 'TALL or WIDE'),
  header_count     INT64 OPTIONS(description = 'Column count from row 3'),
  delimiter        STRING OPTIONS(description = 'Detected delimiter'),
  encoding         STRING OPTIONS(description = 'Detected encoding'),
  row1_raw         STRING OPTIONS(description = 'Preamble row 1 as JSON array string'),
  row2_raw         STRING OPTIONS(description = 'Preamble row 2 as JSON array string'),
  preamble_kv      JSON OPTIONS(description = 'Key-value pairs parsed from row 1'),
  rows_loaded      INT64 OPTIONS(description = 'Rows loaded into pt_csv_raw_tall or pt_csv_raw_wide'),
  status           STRING OPTIONS(description = 'success or failed'),
  error_message    STRING OPTIONS(description = 'Error if failed')
)
OPTIONS(
  description = 'File-level registry for CSV PT files; preamble captured once per file.'
);

-- -----------------------------------------------------------------------------
-- pt_json_registry: registry of JSON MRF files (run, paths, status, row counts)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pt_analytics.pt_json_registry (
  source_file_name STRING NOT NULL OPTIONS(description = 'Original file name'),
  local_path       STRING OPTIONS(description = 'Local filesystem path when ingested'),
  gcs_uri          STRING OPTIONS(description = 'GCS URI if uploaded'),
  ingested_at      TIMESTAMP NOT NULL OPTIONS(description = 'When the file was registered'),
  run_id           STRING OPTIONS(description = 'Bulk run identifier (e.g. UTC timestamp)'),
  status           STRING OPTIONS(description = 'success or failed'),
  rows_extracted   INT64 OPTIONS(description = 'Rows emitted by extract to NDJSON'),
  rows_loaded      INT64 OPTIONS(description = 'Rows loaded into pt_json_extracted_rates'),
  file_size_bytes  INT64 OPTIONS(description = 'File size in bytes for traceability'),
  file_sha256      STRING OPTIONS(description = 'SHA-256 hex digest of file'),
  error_message    STRING OPTIONS(description = 'Traceback or error if failed')
)
OPTIONS(
  description = 'Registry of JSON price transparency files for scalable MRF pipeline.'
);

-- -----------------------------------------------------------------------------
-- pt_json_extracted_rates: stream-extracted rate records from large JSON MRF files
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pt_analytics.pt_json_extracted_rates (
  source_file_name     STRING NOT NULL OPTIONS(description = 'Original JSON file name'),
  run_id               STRING OPTIONS(description = 'Bulk run identifier when loaded'),
  ingested_at          TIMESTAMP NOT NULL OPTIONS(description = 'When the extract was run'),
  record_path          STRING OPTIONS(description = 'JSON path that produced this record (lineage)'),
  billing_code         STRING OPTIONS(description = 'CPT/HCPCS/DRG or other code'),
  billing_code_type    STRING OPTIONS(description = 'Code type or system'),
  description          STRING OPTIONS(description = 'Service/item description'),
  payer                STRING OPTIONS(description = 'Payer name or id'),
  plan                 STRING OPTIONS(description = 'Plan name or id'),
  negotiated_rate      NUMERIC OPTIONS(description = 'Negotiated rate value'),
  rate_type            STRING OPTIONS(description = 'Rate type e.g. negotiated, cash'),
  billing_class        STRING OPTIONS(description = 'Inpatient, outpatient, etc.'),
  payment_methodology  STRING OPTIONS(description = 'Payment methodology'),
  payment_unit         STRING OPTIONS(description = 'Per unit, per day, etc.'),
  raw_rate             JSON OPTIONS(description = 'Full extracted object for traceability')
)
OPTIONS(
  description = 'Extracted rate records from JSON MRF; populated via load job from NDJSON.'
);
