# BigQuery cleanup and runbook

Safe commands to inspect and clean up BigQuery datasets used by this project. **Do not delete the marts dataset** if the Streamlit app or BI tools depend on it.

---

## Final intended datasets and tables

| Dataset | Purpose | Required for Streamlit |
|--------|---------|------------------------|
| **pt_analytics_marts** | Star schema marts (dim/fct) | **Yes — do not delete** |
| pt_analytics_staging | dbt staging views (optional) | No |
| pt_analytics_intermediate | dbt intermediate tables (optional) | No |

### Required marts tables

The Streamlit app (BigQuery mode) and BI tools expect these tables in the **marts** dataset:

- **dim_hospital**
- **dim_payer**
- **dim_procedure**
- **fct_standard_charges_semantic**
- **dim_source_file** (if present in your dbt marts)

If any of these are missing, the app will show errors when using BigQuery as the data source.

---

## Commands

Replace `<project>` with your GCP project ID (e.g. `pricing-transparency-portfolio`) and `<dataset>` with the dataset name.

### List datasets in the project

```bash
bq ls <project>
```

### List tables in a dataset

```bash
bq ls <project>:<dataset>
```

Example:

```bash
bq ls my-project:pt_analytics_marts
```

### Delete a dataset (irreversible)

**Warning:** This permanently deletes the dataset and all tables inside it. Use only when you intend to remove the dataset entirely.

```bash
bq rm -r -f <project>:<dataset>
```

- `-r` removes the dataset and all its contents.
- `-f` skips the confirmation prompt (omit for interactive confirmation).

Example (do not run unless you intend to delete):

```bash
# Optional / non-marts only — safe only if you no longer need staging/intermediate
bq rm -r -f my-project:pt_analytics_staging
bq rm -r -f my-project:pt_analytics_intermediate
```

---

## Do not delete marts

- **Do not** run `bq rm -r -f <project>:pt_analytics_marts` unless you are intentionally tearing down the app’s BigQuery backend.
- The Streamlit app (BigQuery mode) and any dashboards (e.g. Power BI) read from **pt_analytics_marts**. Deleting it will break the app until you re-run dbt to rebuild the marts.

---

## Recreating marts after cleanup

If you deleted the marts dataset and need to restore it:

1. Set `GOOGLE_APPLICATION_CREDENTIALS` and `DBT_BQ_PROJECT` / `DBT_BQ_DATASET` (or configure `dbt/profiles.yml`).
2. From repo root run the BigQuery gold build, e.g. `scripts/run_bigquery_gold.ps1` (or the equivalent dbt commands from `dbt/`).
3. Confirm tables exist: `bq ls <project>:pt_analytics_marts`.

See [bigquery_publish.md](bigquery_publish.md) and [bi/bigquery_publish.md](bi/bigquery_publish.md) for full publish steps.
