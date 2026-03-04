# Runbook: Bronze re-ingest, Header sniffing, Reruns, Common issues

## Bronze re-ingest (local)

If CSV files were ingested with the **wrong header row** (e.g. preamble rows treated as header so Silver has no CSV rows), force re-ingest so Bronze is re-read and header detection runs again:

```powershell
.\scripts\reingest_local_bronze.ps1 -IngestDate YYYY-MM-DD -Sources pt_csv -Force
```

Then rebuild Silver for that date and re-run local dbt/export if needed.

## Header sniffing (CSV)

Many hospital CSVs have **2 preamble rows**; the **charge table header is on row 3** (1-based). Bronze ingestion **auto-detects** the header by scoring candidate rows for code/rate column names. Diagnostics show the chosen header, e.g. `header_row=2 (1-based: 3) score=40` and `code_col=yes rate_col=yes => CHARGE TABLE`. If you see CSV/tabular=0 in Silver, the header was likely wrong — use **Bronze re-ingest (local)** above with `-Force`, then rebuild Silver.

## Reruns

1. **Bronze ingest failed for a date**
   - In Dagster UI: re-run asset `ingest_bronze` for partition `YYYY-MM-DD`.
   - Or: `python -c "from ingestion.bronze_ingest import run_bronze_ingest; run_bronze_ingest(ingest_date='YYYY-MM-DD')"`.
   - Manifest: SUCCESS rows are skipped on retry; FAILED rows are retried.

   **Force re-ingest (local):** If files were previously ingested but only preamble/metadata was captured (e.g. CSVs skipped and Silver shows CSV/tabular=0), re-process files for that date ignoring the manifest:
   ```powershell
   .\scripts\reingest_local_bronze.ps1 -IngestDate 2026-03-03 -Sources pt_csv -Force
   ```
   Then run Silver and local BI as usual. See README "How to force re-ingest bronze" for details.

2. **Silver build failed**
   - Re-run `build_silver` for that partition; reads Bronze again and overwrites Silver partition.

3. **DQ gate failed (quarantine_rate > threshold)**
   - Inspect `lake/silver/quarantine/ingest_date=YYYY-MM-DD/` and fix upstream data or relax `DQ_QUARANTINE_RATE_THRESHOLD`.
   - Then re-run from `build_silver` (or fix Bronze and re-run Bronze -> Silver).

4. **dbt build or dbt test failed**
   - Fix dbt models/tests or data; then re-run `dbt_build_gold` and `dbt_test_gate` for the partition (or run `dbt build` / `dbt test` locally).

## Backfills

- **Full backfill (all dates):** In Dagster, launch backfill for `daily_lakehouse_job` with partition range.
- **Single date:** Run partition `YYYY-MM-DD` for the job.
- **Bronze-only backfill:** Run only `ingest_bronze` for the desired partitions.

## Schema drift

- **New columns in raw:** Update `transform/silver_build.py` `_standardize_row` and `CANONICAL_COLUMNS`; then dbt staging/silver sources if Gold reads from Silver.
- **dbt contract change:** Update marts/intermediate and run `dbt run --full-refresh` as needed.

## Failure scenarios (common issues)

| Failure            | Action                                                |
|--------------------|--------------------------------------------------------|
| No files in raw_drop | Add files to `data/raw_drop/` or fix GCS listing.   |
| Bronze write error | Check disk/GCS permissions and path.                   |
| Silver empty / CSV rows = 0 | Wrong CSV header detected. Force re-ingest Bronze (see above), then rebuild Silver. |
| Quarantine rate high | Inspect `lake/silver/quarantine/`; fix validation or data. |
| dbt build fail     | Check `dbt/profiles.yml` (copy from `dbt/profiles.template.yml`), project/dataset, credentials. Run `dbt parse`. |
| dbt test fail      | Fix data or model logic; re-run tests.                 |
| Streamlit "Local data missing" | Run Bronze → Silver → dbt build → export (or `scripts/mvp_local.ps1`). Ensure `dbt/exports/` has required tables. |

## Env vars (recap)

- `EXECUTION_MODE`: `bq` (default) or `local`.
- `STORAGE_BACKEND`: `local` or `gcs`.
- `LAKE_BASE_DIR`: Project root for local lake paths.
- `GCS_BUCKET`, `GCS_PREFIX`: For GCS backend.
- `DQ_QUARANTINE_RATE_THRESHOLD`: Default 0.5; gate fails if exceeded.
- `DBT_PROJECT_DIR`: Path to dbt project (default `dbt`).
- `GOOGLE_APPLICATION_CREDENTIALS`: For GCS/BigQuery when not local.
