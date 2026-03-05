-- BigQuery inventory: list tables per dataset with type, creation_time, last_modified_time.
-- For row_count and size_bytes use Section 3 (TABLE_STORAGE, region-US).
-- No deletes. Replace pricing-transparency-portfolio with your project if needed.

-- =============================================================================
-- Section 1: All datasets in one result set (TABLES: name, type, times)
-- =============================================================================

SELECT
  'pt_analytics_marts' AS dataset_name,
  table_name,
  table_type,
  TIMESTAMP_MILLIS(creation_time) AS creation_time,
  TIMESTAMP_MILLIS(last_modified_time) AS last_modified_time
FROM `pricing-transparency-portfolio.pt_analytics_marts.INFORMATION_SCHEMA.TABLES`
WHERE table_type IN ('BASE TABLE', 'VIEW')

UNION ALL

SELECT
  'pt_analytics_intermediate',
  table_name,
  table_type,
  TIMESTAMP_MILLIS(creation_time),
  TIMESTAMP_MILLIS(last_modified_time)
FROM `pricing-transparency-portfolio.pt_analytics_intermediate.INFORMATION_SCHEMA.TABLES`
WHERE table_type IN ('BASE TABLE', 'VIEW')

UNION ALL

SELECT
  'pt_analytics_staging',
  table_name,
  table_type,
  TIMESTAMP_MILLIS(creation_time),
  TIMESTAMP_MILLIS(last_modified_time)
FROM `pricing-transparency-portfolio.pt_analytics_staging.INFORMATION_SCHEMA.TABLES`
WHERE table_type IN ('BASE TABLE', 'VIEW')

UNION ALL

SELECT
  'pt_analytics',
  table_name,
  table_type,
  TIMESTAMP_MILLIS(creation_time),
  TIMESTAMP_MILLIS(last_modified_time)
FROM `pricing-transparency-portfolio.pt_analytics.INFORMATION_SCHEMA.TABLES`
WHERE table_type IN ('BASE TABLE', 'VIEW')

ORDER BY dataset_name, table_name;


-- =============================================================================
-- Section 3: Row counts and sizes (TABLE_STORAGE; region-US – change if needed)
-- =============================================================================

SELECT
  table_schema AS dataset_name,
  table_name,
  total_rows AS row_count_estimate,
  total_logical_bytes AS size_bytes,
  ROUND(total_logical_bytes / POW(10, 6), 2) AS size_mb
FROM `pricing-transparency-portfolio.region-US.INFORMATION_SCHEMA.TABLE_STORAGE`
WHERE table_schema IN ('pt_analytics_marts', 'pt_analytics_intermediate', 'pt_analytics_staging', 'pt_analytics')
ORDER BY table_schema, table_name;


-- =============================================================================
-- Section 2: Separate query per dataset (run one at a time if Section 1 fails)
-- =============================================================================

-- pt_analytics_marts (run alone if Section 1 fails)
/*
SELECT 'pt_analytics_marts' AS dataset_name, table_name, table_type,
  TIMESTAMP_MILLIS(creation_time) AS creation_time,
  TIMESTAMP_MILLIS(last_modified_time) AS last_modified_time
FROM `pricing-transparency-portfolio.pt_analytics_marts.INFORMATION_SCHEMA.TABLES`
WHERE table_type IN ('BASE TABLE', 'VIEW') ORDER BY table_name;
*/

-- pt_analytics_intermediate
/*
SELECT 'pt_analytics_intermediate' AS dataset_name, table_name, table_type,
  TIMESTAMP_MILLIS(creation_time) AS creation_time,
  TIMESTAMP_MILLIS(last_modified_time) AS last_modified_time
FROM `pricing-transparency-portfolio.pt_analytics_intermediate.INFORMATION_SCHEMA.TABLES`
WHERE table_type IN ('BASE TABLE', 'VIEW') ORDER BY table_name;
*/

-- pt_analytics_staging
/*
SELECT 'pt_analytics_staging' AS dataset_name, table_name, table_type,
  TIMESTAMP_MILLIS(creation_time) AS creation_time,
  TIMESTAMP_MILLIS(last_modified_time) AS last_modified_time
FROM `pricing-transparency-portfolio.pt_analytics_staging.INFORMATION_SCHEMA.TABLES`
WHERE table_type IN ('BASE TABLE', 'VIEW') ORDER BY table_name;
*/

-- pt_analytics
/*
SELECT 'pt_analytics' AS dataset_name, table_name, table_type,
  TIMESTAMP_MILLIS(creation_time) AS creation_time,
  TIMESTAMP_MILLIS(last_modified_time) AS last_modified_time
FROM `pricing-transparency-portfolio.pt_analytics.INFORMATION_SCHEMA.TABLES`
WHERE table_type IN ('BASE TABLE', 'VIEW') ORDER BY table_name;
*/
