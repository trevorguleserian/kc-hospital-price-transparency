# BigQuery cleanup plan (manual execution only)

**Nothing is deleted automatically.** Run only the steps you choose, after confirming what to drop using [bigquery_lineage_report.md](bigquery_lineage_report.md) and the inventory/lineage scripts.

---

## Warnings

- **Irreversible:** Dropping a table or dataset cannot be undone (except from time-travel within the retention window; see BigQuery docs).
- **Costs:** Creating backup tables (e.g. `CREATE TABLE AS SELECT`) incurs storage and (for large tables) query cost. Archiving then dropping still uses storage until you delete the archive.
- **App impact:** Do **not** drop any table in **pt_analytics_marts** that the Streamlit app or BI tools use. Minimum required: `dim_hospital`, `dim_payer`, `dim_procedure`, `dim_source_file`, `fct_standard_charges`, `fct_standard_charges_semantic`.

---

## Step 0: Create an archive dataset (recommended)

Create a one-off dataset for backups before dropping anything. Use a date suffix so you can prune old archives later.

```sql
-- Replace YYYYMMDD with today’s date (e.g. 20260226).
CREATE SCHEMA IF NOT EXISTS `pricing-transparency-portfolio.pt_analytics_archive_YYYYMMDD`
OPTIONS(
  location = "US",
  description = "Archive before cleanup YYYYMMDD"
);
```

Or via `bq` CLI:

```bash
bq mk --dataset --location=US pricing-transparency-portfolio:pt_analytics_archive_YYYYMMDD
```

---

## Step 1: Backup a table before drop (optional)

Only for tables you have **already decided** to drop. Replace `DATASET`, `TABLE`, and `YYYYMMDD` with your values.

```sql
-- Backup one table into the archive dataset.
CREATE TABLE `pricing-transparency-portfolio.pt_analytics_archive_YYYYMMDD.DATASET__TABLE` AS
SELECT * FROM `pricing-transparency-portfolio.DATASET.TABLE`;

-- Example: backup a staging table you are about to drop
-- CREATE TABLE `pricing-transparency-portfolio.pt_analytics_archive_20260226.pt_analytics_staging__old_backup` AS
-- SELECT * FROM `pricing-transparency-portfolio.pt_analytics_staging.old_backup`;
```

---

## Step 2: Drop a single table (manual only)

Run only after you have confirmed the table is safe to drop (e.g. not in the “Keep” list, and not referenced by the app or dbt).

```sql
-- Fully qualified DROP. Replace DATASET and TABLE.
DROP TABLE IF EXISTS `pricing-transparency-portfolio.DATASET.TABLE`;

-- Example (do not run unless you identified "my_old_table" as an artifact):
-- DROP TABLE IF EXISTS `pricing-transparency-portfolio.pt_analytics_staging.my_old_table`;
```

---

## Step 3: Drop an entire dataset (high risk)

Only if you have decided to retire a **whole** dataset (e.g. you no longer need staging/intermediate and have rebuilt marts elsewhere, or you are decommissioning the project).

**Do not use for pt_analytics_marts** — the app depends on it.

```bash
# Interactive (prompts for confirmation)
bq rm -r pricing-transparency-portfolio:DATASET

# No prompt (use only when you are certain)
bq rm -r -f pricing-transparency-portfolio:DATASET
```

Example for an **optional** dataset you have confirmed is no longer needed:

```bash
# bq rm -r pricing-transparency-portfolio:pt_analytics_staging
```

---

## Step 4: Drop archive dataset later (optional)

After you have verified you do not need the backup, you can remove the archive dataset to reduce storage:

```bash
bq rm -r -f pricing-transparency-portfolio:pt_analytics_archive_YYYYMMDD
```

---

## Summary: minimum required for the Streamlit app

- **Dataset:** `pt_analytics_marts`
- **Tables (do not drop):**
  - dim_hospital  
  - dim_payer  
  - dim_procedure  
  - dim_source_file  
  - fct_standard_charges  
  - fct_standard_charges_semantic  

Anything not in this list may be a candidate for cleanup only after you have confirmed it is unused (see [bigquery_lineage_report.md](bigquery_lineage_report.md)). When in doubt, **do not drop** (“Do Not Touch Yet”).
