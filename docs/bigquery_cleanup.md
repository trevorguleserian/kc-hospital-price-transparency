# BigQuery cleanup and runbook

Safe steps to inspect and clean up BigQuery datasets. **Nothing is deleted automatically.** Use the BigQuery UI or `bq` CLI only after you confirm what to remove.

---

## Datasets and what the app needs

| Dataset | Purpose | Required for Streamlit |
|--------|---------|------------------------|
| **pt_analytics_marts** | Star schema marts (dim/fct) | **Yes — do not delete** |
| pt_analytics_staging | dbt staging views | Optional (lineage/debug) |
| pt_analytics_intermediate | dbt intermediate tables | Optional (lineage/debug) |
| pt_analytics | Base / raw-style datasets | Optional |

**Default marts dataset name:** `pt_analytics_marts` (configurable via secrets `BQ_DATASET_MARTS` or env).

### Required marts tables

The Streamlit app (BigQuery mode) reads from the marts dataset:

- **dim_hospital**
- **dim_payer**
- **dim_procedure**
- **fct_standard_charges_semantic**
- **dim_source_file** (if present in your dbt marts)

If any are missing, BigQuery mode will show errors.

---

## Inventory and lineage

- **scripts/bq_inventory.sql** — Lists tables per dataset (type, creation_time, last_modified_time). Section 3 adds row_count and size_bytes (TABLE_STORAGE, region-US). No deletes.
- **scripts/bq_lineage.sql** — Referenced tables from jobs (optional) and dbt-based relation list for comparison.
- **docs/bigquery_lineage_report.md** — Dependency graph, expected tables, Keep / Consider Dropping / Do Not Touch, and minimum required for the app.
- **docs/bigquery_cleanup_plan.md** — Manual backup (archive dataset) and DROP snippets only; run after you decide what to remove.

```bash
bq query --use_legacy_sql=false < scripts/bq_inventory.sql
```

Run Section 1 and Section 3 separately if your client does not support multiple statements. In BigQuery Studio, open the script, set your project, and run each section.

---

## Safe cleanup steps (manual only)

**1. List datasets**

- **BigQuery UI:** Explore → select your project → see dataset list.
- **CLI:** `bq ls <project>`

**2. List tables in a dataset**

- **BigQuery UI:** Open a dataset → see table list.
- **CLI:** `bq ls <project>:<dataset>`

**3. Confirm before deleting**

- Decide which dataset(s) to remove. Do **not** delete **pt_analytics_marts** if the Streamlit app or BI tools use it.
- Optional datasets (staging/intermediate) can be removed if you no longer need lineage or debug.

**4. Delete only when intended (irreversible)**

- **BigQuery UI:** Select dataset → Delete dataset (confirm).
- **CLI:** `bq rm -r -f <project>:<dataset>`  
  (`-r` = remove dataset and contents; `-f` = no prompt; omit `-f` for interactive confirmation.)

Example (only for optional datasets you have decided to remove):

```bash
bq rm -r -f pricing-transparency-portfolio:pt_analytics_staging
bq rm -r -f pricing-transparency-portfolio:pt_analytics_intermediate
```

---

## Do not delete marts

- **Do not** delete **pt_analytics_marts** unless you are intentionally tearing down the BigQuery backend.
- The Streamlit app (BigQuery mode) reads from the marts dataset. Deleting it will break the app until you re-run dbt to rebuild.

---

## Recreating marts after cleanup

1. Set `GOOGLE_APPLICATION_CREDENTIALS` and `DBT_BQ_PROJECT` / `DBT_BQ_DATASET` (or `dbt/profiles.yml`).
2. From repo root: `scripts/run_bigquery_gold.ps1` (or equivalent dbt build from `dbt/`).
3. Confirm: `bq ls <project>:pt_analytics_marts`.

See [bigquery_publish.md](bigquery_publish.md) and [bi/bigquery_publish.md](bi/bigquery_publish.md) for publish steps.
