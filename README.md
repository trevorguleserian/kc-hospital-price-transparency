# KC Hospital Price Transparency

Analytics pipeline and Streamlit app for hospital price transparency data: ingest raw charge files (CSV/JSON), normalize in a lakehouse (Bronze/Silver), transform with dbt into a star schema (DuckDB locally or BigQuery), and serve search, comparison, and data-quality views.

---

## Project overview

**Problem:** Hospitals publish machine-readable standard charge files in varied formats—CSV with preamble rows, JSON with nested payers—making comparison and analysis difficult.

**Solution:** This project ingests raw files into a **Bronze** layer (Parquet), standardizes to a canonical **Silver** schema (one row per rate, with quarantine for invalid rows), builds a **Gold** star schema in dbt (DuckDB locally or BigQuery in production), and exposes a **Streamlit** app for overview metrics, search & compare, hospital profiles, and data-quality checks.

**Stack:** Python (ingestion, Silver transform), Parquet lake (Bronze/Silver), dbt (staging → intermediate → marts), Streamlit (reads from `dbt/exports/` or BigQuery).

---

## Architecture (Bronze / Silver / Gold + dbt)

Data flows **Raw → Bronze (Parquet) → Silver (standardized + quarantine) → dbt Gold (star schema) → Streamlit**.

| Stage     | Purpose                    | Output / location                    |
|----------|----------------------------|--------------------------------------|
| **Bronze** | Ingest raw CSV/JSON as-is; idempotent by file hash | `lake/bronze/pt_csv/`, `lake/bronze/pt_json/` |
| **Silver** | One row per rate; canonical columns; invalid rows to quarantine | `lake/silver/standard_charges/`, `lake/silver/quarantine/` |
| **dbt Gold** | Staging → intermediate → marts (star schema) | `fct_standard_charges_semantic`, `dim_hospital`, `dim_payer`, `dim_procedure` (DuckDB or BigQuery) |
| **Streamlit** | Search, compare, DQ views  | Reads `dbt/exports/` (local) or BigQuery marts |

**Diagram and design choices:** [docs/architecture.md](docs/architecture.md)

---

## Local quickstart (full)

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

3. **Bronze ingest**  
   Place raw CSV/JSON in `data/raw_drop/`, then:
   ```powershell
   $env:STORAGE_BACKEND = "local"
   $env:PYTHONPATH = (Get-Location).Path
   $IngestDate = "2026-03-03"   # or your date
   python -c "from ingestion.bronze_ingest import run_bronze_ingest; print(run_bronze_ingest(ingest_date='$IngestDate'))"
   ```

4. **Silver build**
   ```powershell
   python -c "from transform.silver_build import build_silver_for_date; print(build_silver_for_date('$IngestDate', base_dir='.'))"
   ```

5. **dbt local build and exports**
   ```powershell
   $env:DBT_SILVER_GLOB = "$PWD\lake\silver\standard_charges\**\*.parquet"
   cd dbt
   dbt deps
   dbt build --target local_duckdb --vars "{execution_mode: local}"
   dbt run-operation export_bi_outputs --target local_duckdb --vars "{execution_mode: local}"
   cd ..
   ```
   Exports land in `dbt/exports/` (Parquet + CSV).

6. **Streamlit (demo mode)**  
   From repo root:
   ```powershell
   streamlit run apps/streamlit_app/Home.py
   ```
   The app defaults to **Local**: it reads from `dbt/exports/` and needs no credentials. Use the sidebar **Data source** to switch to **BigQuery** if you have credentials.

**Shortcut (if Silver already exists):**  
`.\scripts\mvp_local.ps1` runs local dbt build + export + Streamlit. Use `-ForceReingest` to re-ingest Bronze and rebuild Silver first.

---

## BigQuery quickstart

1. **Environment variables**
   - `GOOGLE_APPLICATION_CREDENTIALS` — path to service account JSON key file (or use `gcloud auth application-default login`).
   - Optional: `DBT_BQ_PROJECT`, `DBT_BQ_DATASET` for project/dataset.

2. **dbt profile (no secrets in repo)**  
   Copy the **safe template** to `dbt/profiles.yml` (do not commit):
   ```powershell
   copy dbt\profiles.template.yml dbt\profiles.yml
   ```
   Or copy from [docs/dbt_profiles_template.yml](docs/dbt_profiles_template.yml). Edit `dbt/profiles.yml` and set your `project` and `dataset`; keep the file in `.gitignore`.

3. **Build Gold in BigQuery**  
   From repo root:
   ```powershell
   .\scripts\run_bigquery_gold.ps1
   ```
   Or from `dbt/`: `dbt deps` then `dbt build --target bigquery --select "+path:models/marts"`.

4. **Run Streamlit against BigQuery**  
   Set **Data source** to **BigQuery** in the sidebar, or:
   ```powershell
   $env:APP_MODE = "bigquery"
   streamlit run apps/streamlit_app/Home.py
   ```

**Docs:** [docs/bigquery_publish.md](docs/bigquery_publish.md)

---

## Streamlit MVP (demo mode)

- **Default:** App uses **Local** and reads from `dbt/exports/` (Parquet or CSV). **No credentials required.**  
  Filters: hospital, payer, procedure (billing code), and optionally billing_code_type, rate_category. Results show a top-N table (configurable max rows).

- **If exports are missing:** The app shows a short message and steps to generate them (Bronze ingest → Silver build → dbt build + export, or run `.\scripts\mvp_local.ps1`).

- **BigQuery:** In the sidebar, set **Data source** to **BigQuery** when credentials are configured; the app will query your BigQuery marts.

**Run (from repo root):**
```powershell
streamlit run apps/streamlit_app/Home.py
```

---

## Troubleshooting

| Issue | What to do |
|-------|------------|
| No or wrong CSV rows in Silver | Many hospital CSVs use row 3 as the data header. [Force re-ingest](docs/runbook.md) Bronze (CSV) then rebuild Silver. |
| Quarantine rate high / DQ gate fails | Inspect `lake/silver/quarantine/`; fix data or validation. See [data quality](docs/data_quality.md). |
| dbt build fails (BigQuery) | Ensure `dbt/profiles.yml` exists (copy from `dbt/profiles.template.yml`), correct project/dataset and credentials. Run `dbt parse` to validate. |
| dbt build fails (local) | Set `DBT_SILVER_GLOB` to your Silver Parquet glob. Use target `local_duckdb` and `execution_mode: local`. |
| Streamlit "BigQuery unavailable" | Set `GOOGLE_APPLICATION_CREDENTIALS` or run `gcloud auth application-default login`. For demo, use **Local** in the sidebar. |
| Streamlit "Local data missing" | Run Bronze → Silver → dbt build → export (or `.\scripts\mvp_local.ps1`). Ensure `dbt/exports/` contains at least `dim_hospital` and `fct_standard_charges_semantic` (as .parquet or .csv). |
| dbt not found | `pip install dbt-core dbt-bigquery` (BigQuery) or `dbt-core dbt-duckdb` (local). |

**Full runbook:** [docs/runbook.md](docs/runbook.md) — bronze re-ingest, header sniffing, common issues, env vars.

---

## Documentation

| Doc | Description |
|-----|-------------|
| [docs/architecture.md](docs/architecture.md) | Pipeline diagram, stages, design choices. |
| [docs/runbook.md](docs/runbook.md) | Bronze re-ingest, header sniffing, failures, backfills, env vars. |
| [docs/bigquery_publish.md](docs/bigquery_publish.md) | BigQuery publish steps and validation. |
| [docs/data_quality.md](docs/data_quality.md) | Quarantine reason codes, dbt tests, limitations. |
| [docs/dbt_profiles_template.yml](docs/dbt_profiles_template.yml) | dbt profile template (safe). Copy to `dbt/profiles.yml`; do not commit. |

---

## Project structure

| Path | Purpose |
|------|---------|
| `apps/streamlit_app/` | Streamlit app (Home, Search & Compare, Hospital Profile, Data Quality). |
| `dbt/` | dbt project (staging, intermediate, marts). Use `dbt/profiles.template.yml` → copy to `dbt/profiles.yml`. |
| `ingestion/` | Bronze ingest (CSV/JSON → Parquet). |
| `transform/` | Silver build (standardize + quarantine). |
| `lake/` | Bronze and Silver Parquet (local; not committed). |
| `scripts/` | `mvp_local.ps1`, `run_bigquery_gold.ps1`, `reingest_local_bronze.ps1`, `run_local_bi.ps1`. |
| `docs/` | Architecture, runbook, BigQuery publish, data quality. |
