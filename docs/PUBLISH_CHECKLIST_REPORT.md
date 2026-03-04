# Publish-ready checklist report

Final verification and changes made for a publish-ready GitHub repo.

---

## 1) Repo hygiene verification

**Checked:**

- **.gitignore** — Confirmed it excludes:
  - `data/raw_drop/`, `incoming/`, `lake/`, `warehouse/`, `dbt/target/`, `dbt/logs/`, `dbt/exports/`, `logs/`
  - `*.parquet`, `*.ndjson`, `*.csv` with `!data/sample/` and `!data/sample/**` so only `data/sample/` can commit those extensions
  - `dbt/profiles.yml`, `**/profiles.yml`, `.env`, `**/*service-account*.json`, `**/*.key`, `**/*.pem`, `**/secrets*`
- **dbt/profiles.yml** — Not tracked; listed in .gitignore. Template is `dbt/profiles.template.yml` (different name).
- **Secrets-like filenames** — Preflight script fails if any tracked file matches profiles.yml (non-template), *.key, *.pem, *service-account*.json, .env (non-example). CI now runs preflight.

**No changes made** to .gitignore; it already met the checklist.

---

## 2) Reproducibility

**Checked:**

- **README Quickstart** — Commands are correct for Windows PowerShell: `cd kc-hospital-price-transparency`, `.\scripts\mvp_local.ps1` with `$env:SAMPLE_DATA = "1"`. Script sets `PYTHONPATH`, `STORAGE_BACKEND`, `APP_MODE`, and `DBT_SILVER_GLOB` internally.
- **scripts/mvp_local.ps1** — With `SAMPLE_DATA=1` it runs Bronze ingest (from `data/sample/` via ingestion code), Silver build, then `run_local_bi.ps1` (dbt build + export), then Streamlit. No credentials required. Prerequisites: `pip install -r apps/streamlit_app/requirements.txt` and `pip install dbt-core dbt-duckdb`.

**No script changes**; README already matched behavior.

---

## 3) Streamlit MVP readiness

**Checked:**

- **Data source** — App uses `dbt/exports` in Local mode (sidebar “Local (demo)”); BigQuery optional. `lib/data.py` reads from `_local_export_dir()` (default `dbt/exports`) with Parquet/CSV fallback.
- **requirements.txt** — Contains streamlit, pandas, pyarrow, google-cloud-bigquery, python-dotenv. Sufficient for local and BigQuery modes.

**Changes made:**

- **README** — Added section **“Run Streamlit locally”** with exact command: `streamlit run apps/streamlit_app/Home.py` and a short note on Local/Sample mode and sidebar.
- **README** — Added section **“Deploy to Streamlit Community Cloud”** with entrypoint (`streamlit run apps/streamlit_app/Home.py`), root directory, BigQuery vs Local/sample options, and requirements reference.

---

## 4) CI (lightweight)

**Checked:**

- **.github/workflows/ci.yml** — Already had: checkout, Python 3.11, Streamlit deps, data-layer import smoke test, dbt deps + parse with a placeholder profile (no real credentials).

**Changes made:**

- **CI** — Added step **“Preflight repo audit”** that runs `bash scripts/preflight_repo_audit.sh` before other steps. Ensures no tracked forbidden paths, secrets-like files, or files &gt; 95MB. No credentials or real data required.

---

## 5) Project highlights and next improvements

**Changes made:**

- **README** — Added **“Project highlights”** section with bullets: Bronze → Silver Parquet, dbt marts + tests, semantic fact, preflight audit, force re-ingest + header detection.
- **README** — Added **“Next improvements”** bullets: orchestration (e.g. Dagster), incremental Silver/dbt, more hospitals/sources, stronger DQ rules and alerting.

---

## Files changed

| File | Change |
|------|--------|
| `.github/workflows/ci.yml` | Added “Preflight repo audit” step: `bash scripts/preflight_repo_audit.sh`. |
| `README.md` | Added “Project highlights” and “Next improvements”; added “Run Streamlit locally” and “Deploy to Streamlit Community Cloud” sections. |
| `docs/PUBLISH_CHECKLIST_REPORT.md` | New: this report. |

---

## Commands to validate locally

Run from repo root (PowerShell).

1. **Preflight**
   ```powershell
   .\scripts\preflight_repo_audit.ps1
   ```
   Expected: “Preflight passed: no forbidden paths, no tracked secrets, no files > 95MB.”

2. **Sample quickstart (end-to-end)**
   ```powershell
   $env:SAMPLE_DATA = "1"
   .\scripts\mvp_local.ps1
   ```
   Expected: Bronze from `data/sample/` → Silver → dbt build + export → Streamlit launches; app shows data from `dbt/exports/` in Local mode.

3. **Streamlit only (after a successful quickstart)**
   ```powershell
   streamlit run apps/streamlit_app/Home.py
   ```
   Expected: App loads; sidebar shows “Local (demo)” and “Data available.”

4. **Forbidden paths not tracked**
   ```powershell
   git ls-files | Select-String -Pattern "^data/raw_drop/|^incoming/|^lake/|^warehouse/|^dbt/exports/|^dbt/target/|^logs/"
   ```
   Expected: No output.

5. **Sample data tracked**
   ```powershell
   git ls-files data/sample
   ```
   Expected: `data/sample/README.md`, `data/sample/sample_standardcharges.csv`, `data/sample/sample_standardcharges.json`.
