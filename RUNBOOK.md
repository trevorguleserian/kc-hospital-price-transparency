# Runbook — copy/paste commands

Run all commands from the **repo root** unless noted. Use PowerShell; activate the project venv first (e.g. `.venv\Scripts\Activate.ps1`).

---

## CSV format note

Some hospital CSVs have **2 preamble rows** (e.g. hospital name, metadata); the **charge table header is on row 3** (1-based). Bronze ingestion **auto-detects** the header row by sniffing the first 15 lines and scoring candidate rows 0–4 for code-like and rate-like column names; it picks the row with the highest score and uses it for `pd.read_csv(header=...)`. Columns are then mapped to canonical names (e.g. `code|1` -> `billing_code`, `standard_charge|gross` -> `gross_charge`).

During ingest you will see diagnostics like:

```
[Bronze] raw_drop CSV diagnostics (local):
  example_hospital_standardcharges.csv
    header_row=2 (1-based: 3) score=40
    columns(30): description, code|1, code|1|type, ...
    code_col=yes rate_col=yes => CHARGE TABLE
```

If files were previously ingested with row 1 as header (preamble only), Silver will show CSV/tabular=0. **Re-ingest** with the force script so the correct header is used (see below).

---

## Local force re-ingest CSV for a date

Re-process CSV files for a given ingest date (ignores manifest; use when CSVs were skipped or only preamble was ingested):

```powershell
.\scripts\reingest_local_bronze.ps1 -IngestDate 2026-03-03 -Sources pt_csv -Force
```

---

## Build Silver for a date

Standardize Bronze to Silver (one row per rate, good + quarantine):

```powershell
$env:PYTHONPATH = (Get-Location).Path
python -c "from transform.silver_build import build_silver_for_date; build_silver_for_date('2026-03-03', base_dir='.')"
```

Replace `2026-03-03` with your ingest date.

---

## Run local BI export

Build dbt staging from Silver Parquet and export BI artifacts to `dbt/exports`:

```powershell
$env:DBT_SILVER_GLOB = "$PWD\lake\silver\standard_charges\**\*.parquet"
.\scripts\run_local_bi.ps1
```

---

## Run BigQuery gold

Build marts in BigQuery (staging + marts). Set `GOOGLE_APPLICATION_CREDENTIALS` or use gcloud ADC.

```powershell
.\scripts\run_bigquery_gold.ps1
```

---

## Run Streamlit locally

**Local mode** (DuckDB + `dbt/exports`): use MVP script so exports and Streamlit run in one go (see below). Or run Streamlit only:

```powershell
$env:APP_MODE = "local"
$env:PYTHONPATH = (Get-Location).Path
streamlit run apps/streamlit_app/Home.py
```

**BigQuery mode**: set `APP_MODE=bigquery` and (optionally) `BQ_PROJECT`, `BQ_DATASET`, `GOOGLE_APPLICATION_CREDENTIALS` (or use `.env`), then:

```powershell
$env:APP_MODE = "bigquery"
streamlit run apps/streamlit_app/Home.py
```

---

## MVP one-shot scripts

- **Local pipeline + Streamlit:** Re-ingest (optional), Silver, local BI export, then start Streamlit in local mode.
  ```powershell
  .\scripts\mvp_local.ps1
  ```
  Optional: `-IngestDate 2026-03-03 -ForceReingest` to force CSV re-ingest and build Silver for that date before BI + Streamlit.

- **BigQuery + Streamlit:** Build BigQuery gold, then start Streamlit in bigquery mode.
  ```powershell
  .\scripts\mvp_bigquery.ps1
  ```

See script headers for full usage.
