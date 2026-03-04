# KC Hospital Price Transparency Lakehouse (Bronze/Silver/Gold) + dbt + Streamlit

Analytics pipeline and Streamlit MVP for hospital price transparency data: ingest raw charge files (CSV/JSON), normalize in a **lakehouse (Bronze → Silver Parquet)**, transform with **dbt** into a star schema (DuckDB locally or BigQuery), and serve **Streamlit** search, comparison, and data-quality views. Runs locally with **sample data** (no credentials) or with your own raw files; optional BigQuery for production.

---

## Architecture

**Raw → Bronze (Parquet) → Silver (standardized + quarantine) → dbt Gold (star schema) → Streamlit.**

| Stage     | Purpose                    | Output |
|----------|----------------------------|--------|
| **Bronze** | Ingest raw CSV/JSON as-is; idempotent by file hash | `lake/bronze/pt_csv/`, `lake/bronze/pt_json/` |
| **Silver** | One row per rate; canonical columns; invalid rows to quarantine | `lake/silver/standard_charges/`, `lake/silver/quarantine/` |
| **dbt Gold** | Staging → intermediate → marts (star schema) | `fct_standard_charges_semantic`, `dim_hospital`, `dim_payer`, `dim_procedure` (DuckDB or BigQuery) |
| **Streamlit** | Search, compare, DQ views  | Reads `dbt/exports/` (local) or BigQuery marts |

**Diagram and design choices:** [docs/architecture.md](docs/architecture.md)

---

## Project highlights

- **Bronze → Silver Parquet:** Raw CSV/JSON landed as Parquet; Silver standardizes to one row per rate with canonical columns and quarantines invalid rows (reason codes).
- **dbt marts + tests:** Star schema (semantic fact + dims); not_null, relationships, accepted_values, and a warn-only test for UNKNOWN billing_code_type proportion.
- **Semantic fact:** `fct_standard_charges_semantic` normalizes rate categories (negotiated, gross, cash, min, max, etc.) and filters null `billing_code` for BI-safe joins.
- **Preflight audit:** `scripts/preflight_repo_audit.ps1` / `.sh` fails if forbidden paths (raw_drop, lake, etc.) or secrets-like files are tracked, or any file &gt; 95MB.
- **Force re-ingest + header detection:** CSV header auto-detection (many hospital files use row 3); force re-ingest script to reprocess when the wrong header was chosen.

**Next improvements:** Orchestration (e.g. Dagster) for scheduled ingest; incremental Silver/dbt; more hospitals and sources; stronger DQ rules and alerting.

---

## Quickstart (Sample Data)

Uses the small committed files in `data/sample/` so you can run the pipeline and Streamlit **without downloading full data or any credentials**.

**Prerequisites:** Python 3.10+, PowerShell (or adapt for bash).

1. **Clone and venv**
   ```powershell
   cd kc-hospital-price-transparency
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   ```

2. **Install dependencies**
   ```powershell
   pip install -r apps/streamlit_app/requirements.txt
   pip install dbt-core dbt-duckdb
   ```

3. **Run with sample data**
   ```powershell
   $env:SAMPLE_DATA = "1"
   .\scripts\mvp_local.ps1
   ```
   With `SAMPLE_DATA=1`, the script reads from `data/sample/`, runs Bronze ingest → Silver build → dbt build + export → Streamlit. Open the URL shown; the app reads from `dbt/exports/` (Local mode, no credentials).

---

## Full run (Real Data)

For your own hospital files:

1. **Place raw files** in `data/raw_drop/` (CSV and/or JSON). This directory is **intentionally gitignored**; we never commit raw data.
2. **Optionally** set `RAW_DROP_DIR` to another path, or leave unset to use `data/raw_drop/`.
3. **Bronze ingest** (no `SAMPLE_DATA`):
   ```powershell
   $env:STORAGE_BACKEND = "local"
   $env:PYTHONPATH = (Get-Location).Path
   python -c "from ingestion.bronze_ingest import run_bronze_ingest; print(run_bronze_ingest(ingest_date='YYYY-MM-DD'))"
   ```
4. **Silver build** → **dbt build + export** → **Streamlit** as in the full local quickstart (see [docs/runbook.md](docs/runbook.md)).

**Force re-ingest (e.g. wrong CSV header):**  
`.\scripts\reingest_local_bronze.ps1 -IngestDate YYYY-MM-DD -Sources pt_csv -Force` then rebuild Silver. Many hospital CSVs use row 3 as the data header; see [docs/runbook.md](docs/runbook.md) (header sniffing).

---

## BigQuery quickstart

1. Set `GOOGLE_APPLICATION_CREDENTIALS` (or use `gcloud auth application-default login`).
2. Copy `dbt/profiles.template.yml` to `dbt/profiles.yml`; set your `project` and `dataset`. Do not commit `profiles.yml`.
3. From repo root: `.\scripts\run_bigquery_gold.ps1`
4. Run Streamlit and set **Data source** to **BigQuery** in the sidebar.

**Docs:** [docs/bigquery_publish.md](docs/bigquery_publish.md)

---

## Run Streamlit locally

From the repo root (after generating exports via Quickstart or full run):

```powershell
streamlit run apps/streamlit_app/Home.py
```

The app defaults to **Local / Sample** mode: it reads from `dbt/exports/` (Parquet or CSV) and needs no credentials. Use the sidebar **Data source** to switch to **BigQuery** if you have credentials. If exports are missing, the app shows steps to generate them.

---

## Deploy to Streamlit Community Cloud

- **Entrypoint:** Set the run command to: `streamlit run apps/streamlit_app/Home.py` (main script path).
- **Root:** Use the repository root as the working directory so that `dbt/exports` and `apps/streamlit_app` resolve correctly.
- **Do not ship large data.** For a hosted demo, either:
  - **BigQuery backend:** Add secrets (e.g. `GOOGLE_APPLICATION_CREDENTIALS` or service account JSON) and set `APP_MODE=bigquery`, `BQ_PROJECT`, `BQ_DATASET` in app settings.
  - **Local/Sample mode:** Build exports from `data/sample/` in a one-off job or CI, then run the app with `APP_MODE=local` and point `LOCAL_EXPORT_DIR` to where exports are available (e.g. mounted volume or artifact).
- **Dependencies:** `apps/streamlit_app/requirements.txt` (streamlit, pandas, pyarrow, google-cloud-bigquery, python-dotenv). Python 3.10+.

---

## Security note

- **Raw data and credentials are not committed.**  
  `data/raw_drop/`, `lake/`, `warehouse/`, `dbt/exports/`, and secrets are gitignored.
- Use **`.env.example`** as a template for local env; copy to `.env` and keep `.env` out of git.
- Use **`dbt/profiles.template.yml`**; copy to `dbt/profiles.yml` and never commit `profiles.yml`.

**Repo hygiene:** [docs/repo_hygiene.md](docs/repo_hygiene.md). Before pushing, run **`.\scripts\preflight_repo_audit.ps1`** (or `./scripts/preflight_repo_audit.sh`) to check for tracked secrets or large files.

---

## Troubleshooting

| Issue | What to do |
|-------|------------|
| No or wrong CSV rows in Silver | CSVs often use row 3 as header. [Force re-ingest](docs/runbook.md) Bronze (CSV) then rebuild Silver. |
| Quarantine rate high | Inspect `lake/silver/quarantine/`; see [data quality](docs/data_quality.md). |
| dbt build fails (local) | Set `DBT_SILVER_GLOB` to your Silver Parquet glob; use target `local_duckdb` and `execution_mode: local`. |
| dbt build fails (BigQuery) | Copy `dbt/profiles.template.yml` to `dbt/profiles.yml`, set project/dataset and credentials. |
| Streamlit "Local data missing" | Run Bronze → Silver → dbt build → export (or sample quickstart above). Ensure `dbt/exports/` has required tables. |
| Streamlit "BigQuery unavailable" | Set credentials or use **Local** in the sidebar. |

**Full runbook:** [docs/runbook.md](docs/runbook.md)

---

## Documentation

| Doc | Description |
|-----|-------------|
| [docs/architecture.md](docs/architecture.md) | Pipeline diagram, design choices. |
| [docs/runbook.md](docs/runbook.md) | Bronze re-ingest, header sniffing, failures, env vars. |
| [docs/repo_hygiene.md](docs/repo_hygiene.md) | Why raw_drop/lake are excluded, preflight script, full data. |
| [docs/bigquery_publish.md](docs/bigquery_publish.md) | BigQuery publish and validation. |
| [docs/data_quality.md](docs/data_quality.md) | Quarantine codes, dbt tests, limitations. |

---

## Project structure

| Path | Purpose |
|------|---------|
| `apps/streamlit_app/` | Streamlit app (Home, Search & Compare, Hospital Profile, Data Quality). |
| `data/sample/` | Small sample JSON/CSV for quickstart (committed). |
| `data/raw_drop/` | Your raw files (gitignored). |
| `dbt/` | dbt project; use `profiles.template.yml` → copy to `profiles.yml`. |
| `ingestion/` | Bronze ingest (CSV/JSON → Parquet). |
| `transform/` | Silver build (standardize + quarantine). |
| `scripts/` | `mvp_local.ps1`, `run_bigquery_gold.ps1`, `preflight_repo_audit.ps1`, etc. |
