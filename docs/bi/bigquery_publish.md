# Publishing marts (gold layer) to BigQuery

Steps to build and publish the marts layer to BigQuery and validate row counts.

## Prerequisites

- BigQuery project with datasets for staging/intermediate (e.g. `pt_analytics`) and marts (e.g. `pt_analytics_marts` via dbt schema config).
- Service account key with BigQuery Data Editor and Job User (or equivalent).
- Environment variables set:
  - `GOOGLE_APPLICATION_CREDENTIALS` – path to the service account JSON key file.
  - Optionally `DBT_BQ_PROJECT` and `DBT_BQ_DATASET` (or set project/dataset in `dbt/profiles.yml`).

Use the profile template at `docs/dbt_profiles_template.yml`: copy to `dbt/profiles.yml` (or set `DBT_PROFILES_DIR`) and replace placeholders. Do not commit credentials.

## Run

From the **`dbt/`** directory:

```bash
dbt deps
dbt build --target dev_bigquery --select "+path:models/marts"
```

Or use the selector (marts and all upstream dependencies):

```bash
dbt build --target dev_bigquery --selector marts_plus
```

If your `profiles.yml` uses a different BigQuery target (e.g. `dev`), run:

```bash
dbt build --target dev --select "+path:models/marts"
```

(or `dbt build --target dev --selector marts_plus`).

This builds staging and intermediate models that marts depend on, then the marts. Default `execution_mode` is `bq`, so marts use the BigQuery SQL branch (no DuckDB-specific syntax). Marts that support both backends branch on `var('execution_mode')`; the macro `safe_cast_numeric()` in `macros/safe_cast_numeric.sql` uses adapter dispatch (BigQuery: `safe_cast` to numeric, DuckDB: `try_cast` to double) for shared logic if needed.

## Validate row counts

After the build, run in BigQuery console or `bq query` (replace `YOUR_PROJECT` and dataset if different):

```sql
SELECT 'fct_standard_charges_semantic' AS table_name, COUNT(*) AS row_count
FROM `YOUR_PROJECT.pt_analytics_marts.fct_standard_charges_semantic`
UNION ALL
SELECT 'dim_hospital', COUNT(*) FROM `YOUR_PROJECT.pt_analytics_marts.dim_hospital`
UNION ALL
SELECT 'dim_payer', COUNT(*) FROM `YOUR_PROJECT.pt_analytics_marts.dim_payer`
UNION ALL
SELECT 'dim_procedure', COUNT(*) FROM `YOUR_PROJECT.pt_analytics_marts.dim_procedure`;
```

Or with `bq` CLI:

```bash
bq query --use_legacy_sql=false "
SELECT 'fct_standard_charges_semantic' AS table_name, COUNT(*) AS row_count
FROM \`YOUR_PROJECT.pt_analytics_marts.fct_standard_charges_semantic\`
UNION ALL SELECT 'dim_hospital', COUNT(*) FROM \`YOUR_PROJECT.pt_analytics_marts.dim_hospital\`
UNION ALL SELECT 'dim_payer', COUNT(*) FROM \`YOUR_PROJECT.pt_analytics_marts.dim_payer\`
UNION ALL SELECT 'dim_procedure', COUNT(*) FROM \`YOUR_PROJECT.pt_analytics_marts.dim_procedure\`
"
```

Sanity checks:

- `fct_standard_charges_semantic`: row count greater than zero; no duplicate `semantic_charge_sk` (run a uniqueness check if needed).
- `dim_hospital`: one row per hospital or per source file depending on build; `hospital_id` not null.
- `dim_payer`: distinct (payer_name, plan_name) from the fact source.
- `dim_procedure`: distinct (billing_code, billing_code_type, description) from the fact source.

Compare fact row count to an earlier run or to the intermediate base table to confirm expected volume.

## Where to look in BigQuery

- **Project:** From your profile (e.g. `pricing-transparency-portfolio` or `DBT_BQ_PROJECT`).
- **Marts dataset:** `pt_analytics_marts` (base dataset `pt_analytics` + schema `marts`). If you set `DBT_BQ_DATASET=my_dataset`, marts will be in `my_dataset_marts`.
- **Tables:** `dim_hospital`, `dim_payer`, `dim_procedure`, `fct_standard_charges_semantic`.

**Note (billing_code):** The semantic fact model filters out rows where `billing_code` is null so the published table is BI-safe and the `not_null` test on `billing_code` passes. Rows with null `billing_code` remain in the base union; only the semantic view excludes them.

In BigQuery console: **Explore** → your project → **pt_analytics_marts** → open each table to preview.

## Sample rows (validation)

Run in BigQuery to confirm data shape (replace `YOUR_PROJECT`):

```sql
-- dim_hospital: sample
SELECT * FROM `YOUR_PROJECT.pt_analytics_marts.dim_hospital` LIMIT 5;

-- dim_payer: sample
SELECT * FROM `YOUR_PROJECT.pt_analytics_marts.dim_payer` LIMIT 5;

-- dim_procedure: sample
SELECT * FROM `YOUR_PROJECT.pt_analytics_marts.dim_procedure` LIMIT 5;

-- fct_standard_charges_semantic: sample
SELECT semantic_charge_sk, hospital_id, billing_code, payer_name, plan_name, rate_category, rate_amount
FROM `YOUR_PROJECT.pt_analytics_marts.fct_standard_charges_semantic` LIMIT 5;
```
