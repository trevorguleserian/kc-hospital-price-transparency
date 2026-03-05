-- BigQuery lineage: infer table dependencies where possible.
-- Run sections manually. No deletes.
-- If automatic lineage is limited, use the dbt ref/source mapping at the bottom.

-- =============================================================================
-- Section 1: Referenced tables from recent jobs (optional – may be empty)
-- JOBS_BY_PROJECT is region-specific; use your dataset region (e.g. region-US).
-- =============================================================================

SELECT
  referenced_tables.project_id,
  referenced_tables.dataset_id,
  referenced_tables.table_id,
  COUNT(*) AS reference_count
FROM
  `pricing-transparency-portfolio.region-US.INFORMATION_SCHEMA.JOBS_BY_PROJECT` AS j,
  UNNEST(referenced_tables) AS referenced_tables
WHERE
  creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND statement_type = 'SELECT'
GROUP BY 1, 2, 3
ORDER BY reference_count DESC;


-- =============================================================================
-- Section 2: List all tables with schema (to compare with dbt expected list)
-- =============================================================================

SELECT
  table_catalog AS project_id,
  table_schema AS dataset_id,
  table_name,
  table_type
FROM `pricing-transparency-portfolio.pt_analytics_marts.INFORMATION_SCHEMA.TABLES`
WHERE table_type IN ('BASE TABLE', 'VIEW')
UNION ALL
SELECT table_catalog, table_schema, table_name, table_type
FROM `pricing-transparency-portfolio.pt_analytics_intermediate.INFORMATION_SCHEMA.TABLES`
WHERE table_type IN ('BASE TABLE', 'VIEW')
UNION ALL
SELECT table_catalog, table_schema, table_name, table_type
FROM `pricing-transparency-portfolio.pt_analytics_staging.INFORMATION_SCHEMA.TABLES`
WHERE table_type IN ('BASE TABLE', 'VIEW')
UNION ALL
SELECT table_catalog, table_schema, table_name, table_type
FROM `pricing-transparency-portfolio.pt_analytics.INFORMATION_SCHEMA.TABLES`
WHERE table_type IN ('BASE TABLE', 'VIEW')
ORDER BY dataset_id, table_name;


-- =============================================================================
-- Section 3: dbt-based lineage (source of truth from repo)
-- Map dbt ref() and source() to BigQuery relation names.
-- Profile uses dataset base (e.g. pt_analytics) + schema suffix → pt_analytics_marts, etc.
-- =============================================================================
/*
Expected BigQuery relations (from dbt/models and dbt/models/sources):

  MARTS (dataset pt_analytics_marts):
    dim_hospital          refs: int_standard_charges_base, source(pt_analytics.pt_csv_registry)
    dim_payer             refs: int_standard_charges_base
    dim_procedure         refs: int_standard_charges_base
    dim_source_file       refs: source(pt_analytics.pt_csv_registry), source(pt_analytics.pt_json_registry)
    fct_standard_charges  refs: int_standard_charges_base, dim_hospital
    fct_standard_charges_semantic  refs: fct_standard_charges, int_standard_charges_base, dim_hospital

  INTERMEDIATE (dataset pt_analytics_intermediate):
    int_standard_charges_base     refs: int_pt_standard_charges_union (BQ)
    int_pt_standard_charges_union refs: stg_pt_json_rates, stg_pt_csv_tall, stg_pt_csv_wide

  STAGING (dataset pt_analytics_staging):
    stg_pt_json_rates  refs: source(pt_analytics.pt_json_extracted_rates)
    stg_pt_csv_tall    refs: source(pt_analytics.pt_csv_raw_tall), source(pt_analytics.pt_csv_registry)
    stg_pt_csv_wide    refs: source(pt_analytics.pt_csv_raw_wide), source(pt_analytics.pt_csv_registry)

  SOURCES (dataset pt_analytics):
    pt_csv_registry, pt_csv_raw_tall, pt_csv_raw_wide, pt_json_registry, pt_json_extracted_rates

To regenerate this from dbt: run "dbt compile" and inspect target/manifest.json (node refs),
or grep -r "ref\\|source" dbt/models.
*/
