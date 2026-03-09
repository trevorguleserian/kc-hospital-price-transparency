# KC Hospital Price Transparency

Analytics pipeline and dashboard for **CMS Hospital Price Transparency** data. The project ingests raw standard-charge files (CSV and JSON), normalizes and models them with **dbt** in **BigQuery**, and serves a **Streamlit** app for exploratory analytics and like-to-like comparison of reported hospital standard charges across facilities and payers.

---

## Project Overview

This project analyzes **hospital price transparency** data published under federal rules. Hospitals are required to make standard charges available in machine-readable form; this pipeline consumes those files and supports:

- **Hospital-to-hospital comparison** of rates for the same procedure, payer, and rate type.
- **Payer and plan comparison** to see how negotiated, gross, and cash rates vary by payer and plan across hospitals.
- **Exploratory analytics and comparability analysis** over reported standard charges, with explicit handling of rate categories, units, and harmonized dimensions so that only comparable rows are used in comparison views.

The app is built for analysts and stakeholders who need to explore pricing variation, assess data quality, and run like-to-like comparisons without mixing incompatible rate types or naming conventions.

---

## Business Context

### Hospital Price Transparency Regulation

The **Hospital Price Transparency** rule (CMS-1717-F2, effective January 2021) requires hospitals to disclose standard charges in a consumer-friendly format and as machine-readable data. Hospitals publish files that include negotiated rates with payers, gross charges, discounted cash prices, and minimum/maximum negotiated rates by procedure and plan.

### Why This Data Matters

- **Pricing variation** — Same procedure can have very different rates across hospitals and payers; transparency data makes it possible to quantify this variation.
- **Payer negotiation transparency** — Negotiated rates reveal what payers actually pay; the data supports analysis of contracting and rate spread.
- **Hospital benchmarking** — Facilities and analysts can compare their rates to others for the same procedure and payer/plan.
- **Healthcare analytics** — Researchers and operators use the data for cost modeling, network design, and consumer tools.

This project focuses on a **selected set of hospitals** to keep the pipeline and data model at a practical portfolio scale while demonstrating end-to-end analytics engineering: ingestion, modeling, comparability logic, and an interactive UI.

---

## Current Architecture

### Raw Ingestion Layer

- **CSV raw** — Tall and wide CSV files are landed in BigQuery (e.g. `pt_csv_raw_tall`, `pt_csv_raw_wide`) or in local Parquet under `lake/bronze/pt_csv/`. Each row is typically one rate line (procedure, payer, plan, rate type, amount).
- **JSON raw** — JSON-format price files are landed in BigQuery (`pt_json_extracted_rates` or equivalent) or in local Parquet under `lake/bronze/pt_json/`.
- **Registry tables** — CSV metadata is stored in registries such as `pt_csv_registry`, including preamble rows (e.g. row 1–2) used to extract hospital name and other file-level metadata. This supports clean hospital name extraction from CSV row 2 / preamble.

### dbt Layers

- **Staging** — Views and tables that read from raw sources, parse JSON/CSV columns, and produce a consistent set of columns (e.g. `stg_pt_csv_tall`, `stg_pt_csv_wide`, `stg_pt_json_rates`, `stg_hospital_metadata`). Staging normalizes field names and types for downstream use.
- **Intermediate** — Union and base tables (e.g. `int_pt_standard_charges_union`, `int_standard_charges_base`) that combine all sources into one row-per-rate dataset with columns such as `rate_type`, `rate_amount`, `gross_charge`, `discounted_cash`, `minimum`, `maximum`.
- **Marts** — Dimensions and facts consumed by the Streamlit app and by analysts: `dim_hospital`, `dim_payer`, `dim_payer_harmonized`, `dim_procedure`, `dim_procedure_harmonized`, `fct_standard_charges`, `fct_standard_charges_semantic`, comparability and comparison aggregates (see Data Model below).

### BigQuery as Warehouse

BigQuery is the production warehouse. Raw tables, staging, intermediate, and marts are created in configurable datasets (e.g. `pt_analytics`, `pt_analytics_marts`). The Streamlit app reads from the marts dataset only; ingestion and dbt jobs populate the warehouse.

### Streamlit as Analytics UI

The Streamlit app is the primary user interface. It runs in **BigQuery-only** mode in production (Streamlit Cloud): it does not read from local Parquet. It queries marts for search, comparison, coverage, and data-quality views. Local/demo mode can read from `dbt/exports/` when configured.

### BI / Dashboard layer (Looker Studio)

The **Executive BI Dashboard** page embeds a **Looker Studio** report via iframe. Looker Studio connects to the same BigQuery/dbt marts and provides executive views (CPT/MS-DRG comparisons, hospital-to-hospital pricing variation, harmonized payer and procedure groupings). This approach is easier to host publicly than a self-hosted BI tool (e.g. a local Metabase instance): no private network or VPN is required; the embed URL points to Google’s hosted report.

---

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Python 3.10+ |
| Warehouse | Google BigQuery |
| Transform | dbt Core |
| UI | Streamlit |
| Data frames / I/O | pandas, pyarrow |
| Charts (comparison pages) | matplotlib |
| Auth (Cloud) | Google service account via Streamlit Secrets |
| Version control | Git / GitHub |

Additional dependencies: `google-cloud-bigquery`, `db-dtypes` (BigQuery to pandas type mapping), `python-dotenv` for local env, and optional `dbt-duckdb` and DuckDB for local dbt targets.

---

## Data Model: Important Marts

| Mart | Purpose | Business Value |
|------|---------|----------------|
| **dim_hospital** | One row per hospital (or source file); includes `hospital_id`, `hospital_name_clean`, `source_file_name`. | Stable hospital identity and display names; joins to all fact tables. |
| **dim_payer** | Distinct payer and plan names from the data. | Reference list of payers/plans; used for filters and display. |
| **dim_payer_harmonized** | Payer and plan names normalized to `payer_family` and `plan_family` via seeds and matching. | Enables like-to-like comparison by grouping many raw names into a single family. |
| **dim_procedure** | Distinct billing code and type with descriptions. | Procedure reference and search. |
| **dim_procedure_harmonized** | Canonical procedure descriptions per (billing_code, billing_code_type); variant counts. | Single description per code for comparison and search; supports data-quality checks. |
| **fct_standard_charges** | One row per standard charge from the union; wide rate columns (gross_charge, discounted_cash, etc.) plus `rate_type` and `rate_amount`. | Core fact before semantic normalization. |
| **fct_standard_charges_semantic** | One row per (standard_charge_sk, rate_category). Normalizes `rate_type` into canonical `rate_category` (negotiated, gross, cash, min, max, percentage, other) and emits one row per rate amount. | BI-safe fact with consistent rate categories; filters null billing_code. |
| **fct_rates_comparable** | All semantic rows with non-null rate_category and numeric rate_amount; adds `comparability_key`, `is_comparable`, `comparability_reason`. | Flags which rows are suitable for like-to-like comparison; excludes or labels others. |
| **fct_rates_comparable_harmonized** | Comparable fact joined to `dim_payer_harmonized` for `payer_family` and `plan_family`; same grain as `fct_rates_comparable`. | Fact used by comparison marts; harmonized payer/plan for aggregation. |
| **fct_rates_comparable_rejects** | Rows where `is_comparable = FALSE` (e.g. rate_category = other, excluded_other). | Diagnostics and “why no results” for comparison pages. |
| **agg_hospital_procedure_compare** | Aggregated by (billing_code, billing_code_type, payer_family, plan_family, rate_category, rate_unit, comparability_key, hospital_id); min, max, approximate median, row count. Source: harmonized comparable fact only. | Powers Hospital Comparison page: compare rates by hospital for a given procedure and payer/plan. |
| **agg_payer_plan_compare** | Same grain as above; used for payer/plan-level comparison. | Powers Payer Plan Comparison page and Data Quality coverage matrix. |
| **fct_billing_code_rejects** | One row per (source_layer, billing_code_raw, billing_code_type, billing_code_issue_reason) with row_count. | Diagnostics; invalid codes are not dropped from fct_rates_comparable but are excluded from app aggs. |

---

## Implemented Features and Enhancements

- **BigQuery authentication hardening for Streamlit Cloud** — App validates secrets and connection before running; clear errors when BigQuery is selected but credentials are missing.
- **Safe debug panel** — Optional debug expander (enabled via `DEBUG=1` in secrets) shows which secret keys exist, BigQuery config summary (no secret values), and a simple connectivity check. No private keys or tokens are displayed.
- **Secret validation and AttrDict handling** — Robust handling of Streamlit Secrets and service-account structure for Cloud deployments.
- **Clean hospital name extraction** — Hospital names derived from CSV preamble (e.g. row 2) and registry metadata so `hospital_name_clean` is consistent and human-readable.
- **Procedure description harmonization** — `dim_procedure_harmonized` provides a canonical description per (billing_code, billing_code_type) and variant counts for data-quality review.
- **Payer and plan harmonization** — Seed-driven mapping and normalized matching to `payer_family` and `plan_family` so many raw payer/plan names roll up for comparison.
- **Comparability logic** — Only like-to-like rate comparisons: `rate_category`, `rate_unit`, and `comparability_key` ensure that “other” and incompatible categories are excluded from comparison marts (see Comparability Logic below).
- **Rejects model** — `fct_rates_comparable_rejects` exposes non-comparable rows with `comparability_reason` for troubleshooting and “why no results” explanations.
- **Guardrail tests** — dbt tests ensure `rate_category = 'other'` does not appear in `agg_hospital_procedure_compare` or `agg_payer_plan_compare`.
- **Comparison marts** — `agg_hospital_procedure_compare` and `agg_payer_plan_compare` built from the harmonized comparable fact; they power the Hospital Comparison and Payer Plan Comparison pages.
- **New Streamlit pages** — Hospital Comparison (compare hospitals for a procedure/payer/plan/rate type) and Payer Plan Comparison (compare by payer_family and plan_family) with downloadable tables and charts.
- **Data Quality improvements** — Coverage matrix from `agg_payer_plan_compare` (per-hospital comparable row and distinct code counts); variant flags for procedure descriptions and payer-family mappings.
- **BigQuery type compatibility** — Use of STRING and safe casting (e.g. for varchar-like columns) so dbt and the app work correctly with BigQuery types.

---

## Streamlit App Pages

| Page | Description |
|------|-------------|
| **Home** | Overview narrative (what is price transparency, regulation, why these hospitals, executive summary), high-level metrics, and a stacked bar chart of distinct billing codes by hospital and code type (APC excluded). Uses `hospital_name_clean`; hospitals sorted by total distinct services. |
| **Search & Compare** | Search procedures and filter by billing code, rate category, rate unit, and optional payer/plan and hospitals. Results from `fct_rates_comparable_harmonized` (comparable-only) or from `fct_standard_charges_semantic` (single-hospital). Table and CSV download. |
| **Hospital Profile** | Drill into one hospital: KPIs (total rows, distinct procedures, payer/plan count, median/min/max rate), top procedures, and payer coverage. Reads from semantic fact and dims. |
| **Data Quality** | Null-rate metrics, UNKNOWN billing_code_type count, coverage matrix (from `agg_payer_plan_compare`), procedure and payer variant flags, and top outlier rates. CSV/JSON export. |
| **Hospital Comparison** | Compare min, max, and approximate median rates **by hospital** for a selected procedure, payer family, plan family, rate category, and rate unit. Uses `agg_hospital_procedure_compare`. Table and horizontal bar chart (matplotlib); CSV and PNG download. |
| **Payer Plan Comparison** | Compare rates **by payer_family** and **plan_family** for a procedure and optional hospital filter. Uses `agg_payer_plan_compare`. Payer-level and plan-level tables and charts; CSV and PNG download. |
| **Top Codes by Type** | QA-style view: top billing codes by row count from app-facing marts (valid codes only). Optional filters: billing code type, hospitals. Columns: billing_code, billing_code_type, canonical_description, row_count, hospitals_covered. CSV download. |
| **Executive BI Dashboard** | Embedded Looker Studio report (iframe): executive CPT/MS-DRG comparisons, hospital-to-hospital pricing variation, harmonized payer/procedure groupings. Optional `LOOKER_STUDIO_EMBED_URL` in secrets or env; fallback URL used if unset. |

Comparison pages (Hospital Comparison, Payer Plan Comparison) and the Data Quality coverage matrix **depend on the comparable and harmonized marts** (`fct_rates_comparable`, `fct_rates_comparable_harmonized`, `agg_hospital_procedure_compare`, `agg_payer_plan_compare`). If those are empty, run the recommended dbt sequence below to rebuild the semantic and comparison layers.

---

## Comparability Logic

Hospital price transparency data is difficult to compare directly because:

- Rate types and labels vary by hospital (e.g. “negotiated”, “negotiated_rate”, “estimated_amount”).
- Some rows are “other” or non-standard and should not be mixed with negotiated/gross/cash/min/max.
- Payer and plan names are inconsistent across facilities, so aggregation by raw name would undercount or fragment groups.

This project enforces **like-to-like comparison** as follows:

- **Rate category normalization** — The semantic layer maps source `rate_type` to a canonical `rate_category` (negotiated, gross, cash, min, max, percentage, other). Only negotiated, gross, cash, min, max, and percentage are treated as comparable; “other” is retained for diagnostics but excluded from comparison marts.
- **Rate unit handling** — `rate_unit` (e.g. dollars vs percent) is part of the comparability key so that dollar and percentage rates are not mixed in the same comparison.
- **comparability_key** — A composite key (e.g. `billing_code_type | rate_category | rate_unit`) defines the like-to-like group. Rows with the same key are comparable with each other.
- **is_comparable flag** — Set to TRUE only for allowed rate categories (and optional rules such as rate_unit); FALSE for “other” and unexpected categories. Comparison marts filter on `is_comparable = TRUE` and exclude `rate_category = 'other'`.
- **Harmonized payer/plan dimensions** — `dim_payer_harmonized` maps raw payer/plan names to `payer_family` and `plan_family`. The harmonized fact and comparison marts use these families so that comparisons aggregate across facilities consistently.

---

## Billing Code Audit and Normalization

Billing codes are audited and normalized so app-facing marts use **validated, consistent codes** without silently dropping bad data. Invalid codes remain in the pipeline for diagnostics and in `fct_billing_code_rejects`.

### Audit Approach

- **Diagnostics** (in `dbt/models/marts/diagnostics/`):
  - **diag_code_type_inventory** — Row count, distinct code count, distinct description count per `billing_code_type` from `fct_standard_charges_semantic`; ordered by row count.
  - **diag_code_type_examples** — Top 50 most frequent codes per type with `billing_code`, `description`, `row_count`.
  - **diag_code_type_rule_violations** — Rows that violate expected format rules per type (`billing_code_type`, `billing_code`, `issue_type`, `row_count`).
  - **diag_code_type_filter_path** — Counts per layer (semantic → comparable → harmonized → agg) to see if any code types disappear between layers.

- **Centralized macros** (`dbt/macros/normalize_and_validate_billing_codes.sql`):
  - **normalize_billing_code_for_storage(code_expr, type_expr)** — Normalizes for storage; CPT uses last 5 digits only (e.g. 199213 → 99213); other types trim/uppercase as documented.
  - **is_valid_billing_code(code_expr, type_expr)** — Returns true if the code passes format validation for its type.
  - **billing_code_issue_reason(code_expr, type_expr)** — Returns a string reason when invalid (e.g. `cpt_expected_5_digits`), null when valid.

### Normalization Rules by Code Type

| Type | Rule | Assumption |
|------|------|------------|
| **CPT** | Exactly 5 digits. Malformed leading digits (e.g. 199213) normalize to last 5 digits only for CPT. | Leading zeroes preserved (e.g. 01234). |
| **HCPCS** | Exactly 5 alphanumeric (A–Z, 0–9), uppercase. | Trim only for storage; validation enforces 5 chars. |
| **NDC** | Digits only after stripping dashes/spaces/dots; length 10 or 11. | Canonical NDC format (4-4-2, 5-3-2, or 5-4-1) documented in macro. |
| **REVENUE / RC** | 3 or 4 digits. | CMS revenue codes; document assumption in macro. |
| **CDM** | 3 or 4 digits. | Charge description master; same as RC/REVENUE for validation. |
| **ICD_10_PCS** | 7 alphanumeric, uppercase. | Trim and validate length/pattern. |
| **ICD_10_CM** | 3–7 alphanumeric after removing dots. | Allow valid ICD-10-CM style; dots stripped for validation. |
| **DRG / MS_DRG / APR_DRG / TRIS_DRG** | 3 digits. | Trim and validate. |
| **HIPPS** | 5 alphanumeric, uppercase. | Trim and validate. |
| **APC / EAPG** | 4 digits. | Trim and validate. |
| **CDM / UNKNOWN** | No strict validation; treated as valid (no drop). | Used for display/audit only. |

### Rejects and Diagnostic Marts

- **fct_billing_code_rejects** — One row per (source_layer, billing_code_raw, billing_code_type, billing_code_issue_reason) with `row_count`. Invalid codes from `fct_rates_comparable` are not dropped; they are available here and in the comparable fact with `billing_code_is_valid = false`.
- **App-facing aggs** (`agg_hospital_procedure_compare`, `agg_payer_plan_compare`) **exclude** invalid codes (`billing_code_is_valid = true` only). A singular test **no_invalid_codes_in_app_aggs** fails if any invalid code appears in those aggs.

### Rebuild Order (with Billing Code Changes)

After changing normalization or validation logic, run in this order:

```powershell
cd dbt
dbt deps
dbt seed
dbt run --select stg_hospital_metadata dim_hospital dim_procedure dim_procedure_harmonized
dbt run --select fct_standard_charges_semantic
dbt run --select fct_rates_comparable fct_rates_comparable_harmonized
dbt run --select fct_billing_code_rejects
dbt run --select agg_hospital_procedure_compare agg_payer_plan_compare
dbt run --select diag_code_type_inventory diag_code_type_examples diag_code_type_rule_violations diag_code_type_filter_path
dbt test
```

### Top Codes Page (Streamlit)

The **Top Codes by Type** page is a simple QA-style view:

- **Data source:** `agg_hospital_procedure_compare` (valid codes only).
- **Filters:** Billing code type (optional), hospitals (optional), max rows.
- **Columns:** `billing_code`, `billing_code_type`, `canonical_description`, `row_count`, `hospitals_covered`.
- **Download:** CSV of the current result.

Use it to confirm top codes per type and that descriptions and counts look correct after normalization.

---

## Known Issues and Caveats

- **dbt warnings** — Some dbt warnings may still appear (e.g. unused config path). They do not block runs but can be addressed in a future cleanup.
- **requests/urllib3** — A dependency warning (e.g. urllib3 version) may appear during dbt or BigQuery client use. It is safe to ignore for normal runs; pin versions only if required by policy.
- **Hospital-reported variation** — Naming, coding, and rate-type reporting vary by hospital. The pipeline normalizes where possible, but data quality ultimately depends on what facilities publish.
- **Comparability depends on source quality** — If hospitals do not report standard categories or use many “other” or custom rate types, the number of comparable rows may be limited.

---

## Local Setup and Run Instructions

Use the following order so that the environment, dbt profile, seeds, models, and tests all align.

### 1. Environment

```powershell
cd kc-hospital-price-transparency
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r apps/streamlit_app/requirements.txt
```

The app requirements include Streamlit, pandas, matplotlib, pyarrow, db-dtypes, google-cloud-bigquery, and google-auth. For dbt with BigQuery, ensure `dbt-core` and a BigQuery adapter are installed (e.g. `pip install dbt-bigquery` if not already in the app requirements).

### 2. dbt Profile and BigQuery Credentials

- Copy `dbt/profiles.template.yml` to `dbt/profiles.yml`. Do not commit `profiles.yml`.
- Set `project`, `dataset`, and credentials for BigQuery (e.g. `GOOGLE_APPLICATION_CREDENTIALS` pointing to a service account JSON, or `gcloud auth application-default login`).
- Set `DBT_BQ_PROJECT` and `DBT_BQ_DATASET` if you use env-driven config (e.g. `pt_analytics_marts` for marts).

### 3. dbt Run Sequence

From the **`dbt/`** directory:

```powershell
cd dbt
dbt deps
dbt debug
dbt seed
dbt run --select stg_hospital_metadata dim_hospital dim_procedure dim_procedure_harmonized
dbt run --select fct_standard_charges_semantic
dbt run --select fct_rates_comparable fct_rates_comparable_harmonized
dbt run --select fct_billing_code_rejects
dbt run --select agg_hospital_procedure_compare agg_payer_plan_compare
dbt test
```

- **dbt deps** — Installs packages (e.g. dbt_utils) into `dbt_packages/`. Required before run/test if that folder is missing.
- **dbt debug** — Verifies profile and connection. Fix any errors before continuing.
- **dbt seed** — Loads seeds (e.g. payer/plan mapping) into the configured dataset.
- **Semantic fact first** — Rebuilding `fct_standard_charges_semantic` ensures rate categories are correct. If comparison marts are empty, rebuild this layer before the comparable and aggregate layers.
- **Comparable and aggregates** — Build comparability and comparison marts in the order above so dependencies are satisfied.
- **dbt test** — Runs data tests (not_null, relationships, accepted_values, and guardrail tests that ensure “other” does not leak into comparison marts).

Optional: run only comparison marts and their guardrail tests:

```powershell
dbt run --select agg_hospital_procedure_compare agg_payer_plan_compare
dbt test --select no_other_in_agg_hospital_procedure_compare no_other_in_agg_payer_plan_compare no_invalid_codes_in_app_aggs
```

### 4. Run Streamlit

From the repository root:

```powershell
streamlit run apps/streamlit_app/Home.py
```

The app reads from BigQuery when credentials and dataset are configured. Use the sidebar to confirm the active data source.

#### Testing the Executive BI Dashboard page

The **Executive BI Dashboard** page embeds a Looker Studio report via iframe. To test it locally:

1. **Optional — override the embed URL:** The page uses a default Looker Studio embed URL if nothing is configured. To use a different report:
   - **Streamlit secrets:** In `.streamlit/secrets.toml` add:
     ```toml
     LOOKER_STUDIO_EMBED_URL = "https://lookerstudio.google.com/embed/reporting/c2676e11-d089-4281-b4f5-ea81f03603d1/page/RhcrF"
     ```
     You can copy from `.streamlit/secrets.template.toml`. Do not overwrite other keys in `secrets.toml`; add `LOOKER_STUDIO_EMBED_URL` alongside existing BigQuery secrets.
   - **Environment variable:** Set `LOOKER_STUDIO_EMBED_URL` in your shell or `.env` (e.g. `$env:LOOKER_STUDIO_EMBED_URL = "https://..."` in PowerShell).
2. Run the app as above, then open **Executive BI Dashboard** from the sidebar.

No local BI server is required; the default embed URL points to a hosted Looker Studio report, which is easier to use in production and on Streamlit Cloud than a previous local Metabase setup.

---

## BigQuery and Streamlit Cloud Notes

- **Streamlit Cloud secrets** — For BigQuery mode on Streamlit Community Cloud, configure **Secrets** (e.g. in Settings) with at least: `gcp_service_account` (full JSON of the service account key), `BQ_PROJECT`, `BQ_DATASET_MARTS`, and optionally `BQ_LOCATION`. The app does not use local credential files on Cloud.
- **matplotlib** — The Hospital Comparison and Payer Plan Comparison pages use matplotlib for bar charts. The requirements file used by the app (`apps/streamlit_app/requirements.txt`) includes `matplotlib>=3.8,<4`. On Streamlit Cloud, set the **Requirements file** to this path so matplotlib is installed; otherwise those pages may raise `ModuleNotFoundError: No module named 'matplotlib'`.
- **Redeploy after changes** — After updating dependencies or secrets, trigger a redeploy or reboot of the app so the new environment is used.

Detailed Cloud setup (exact TOML structure, IAM roles, optional APP_MODE) is in the **Deploy to Streamlit Community Cloud** section below.

---

## Deploy to Streamlit Community Cloud

- **Entrypoint:** `streamlit run apps/streamlit_app/Home.py`
- **Root:** Repository root as the working directory.
- **Dependencies:** `apps/streamlit_app/requirements.txt`. Python 3.10+.

### Streamlit Cloud to BigQuery

BigQuery mode uses **Streamlit Secrets** only. In the Cloud app **Settings → Secrets**, add a TOML block with:

- **gcp_service_account** — Full service account JSON (as a string or TOML table). No local key files on Cloud.
- **BQ_PROJECT** — GCP project ID.
- **BQ_DATASET_MARTS** — Marts dataset (e.g. `pt_analytics_marts`).
- **BQ_LOCATION** — Optional; default `US`.

The service account needs **BigQuery Data Viewer** (or **BigQuery User**) and **BigQuery Job User** so the app can run queries.

Optional: **DEBUG = "1"** in secrets enables the safe debug panel in the sidebar (key names and config only; no secret values).

Optional: **LOOKER_STUDIO_EMBED_URL** — Full embed URL of the Looker Studio report for the "Executive BI Dashboard" page (e.g. from Looker Studio: Share → Embed report). If omitted, the page uses a built-in default embed URL.

### Tables

Ensure the marts dataset and tables exist (run the dbt sequence above from a machine with BigQuery access, or via CI). The app expects at least: `dim_hospital`, `dim_payer`, `dim_procedure`, `fct_standard_charges_semantic`, and for comparison pages: `fct_rates_comparable_harmonized`, `agg_hospital_procedure_compare`, `agg_payer_plan_compare`, plus `dim_payer_harmonized`, `dim_procedure_harmonized`.

---

## Security and Repo Hygiene

- Raw data and credentials are not committed. `data/raw_drop/`, `lake/`, `dbt/exports/`, and secrets are gitignored.
- Use `.env.example` as a template; copy to `.env` and keep `.env` out of version control.
- Use `dbt/profiles.template.yml`; copy to `dbt/profiles.yml` and do not commit `profiles.yml`.

Before pushing, run `.\scripts\preflight_repo_audit.ps1` (or the shell equivalent) to check for tracked secrets or oversized files. See [docs/repo_hygiene.md](docs/repo_hygiene.md).

---

## Troubleshooting

| Issue | Action |
|-------|--------|
| No or wrong CSV rows in Silver | Many CSVs use row 3 as the data header. Use force re-ingest for Bronze (CSV) then rebuild Silver. See [docs/runbook.md](docs/runbook.md). |
| Comparison marts empty | Rebuild semantic first: `dbt run --full-refresh --select fct_standard_charges_semantic`, then `fct_rates_comparable` and `fct_rates_comparable_harmonized`, then the agg models. Check `diag_comparability_funnel` or semantic rate_category diagnostics. |
| dbt build fails (BigQuery) | Verify `profiles.yml`, project/dataset, and credentials. Ensure dataset exists or dbt can create it. |
| Streamlit “BigQuery unavailable” | Set credentials and secrets; use the debug panel (DEBUG=1) to confirm which keys are present and whether the client is created. |
| ModuleNotFoundError: matplotlib | Use `apps/streamlit_app/requirements.txt` as the Cloud requirements file so matplotlib is installed. |

Additional runbooks: [docs/runbook.md](docs/runbook.md), [docs/bigquery_publish.md](docs/bigquery_publish.md), [docs/bigquery_cleanup.md](docs/bigquery_cleanup.md).

---

## Documentation

| Document | Description |
|----------|-------------|
| [docs/architecture.md](docs/architecture.md) | Pipeline diagram and design choices. |
| [docs/runbook.md](docs/runbook.md) | Bronze re-ingest, header detection, failures, env vars. |
| [docs/repo_hygiene.md](docs/repo_hygiene.md) | Why raw_drop/lake are excluded, preflight script. |
| [docs/bigquery_publish.md](docs/bigquery_publish.md) | BigQuery publish and validation. |
| [docs/bigquery_cleanup.md](docs/bigquery_cleanup.md) | BigQuery cleanup: list/delete datasets, required tables. |
| [docs/data_quality.md](docs/data_quality.md) | Quarantine codes, dbt tests, limitations. |

---

## Project Structure

| Path | Purpose |
|------|---------|
| `apps/streamlit_app/` | Streamlit app: Home, Search & Compare, Hospital Profile, Data Quality, Hospital Comparison, Payer Plan Comparison, Executive BI Dashboard (Looker Studio). |
| `data/sample/` | Small sample CSV/JSON for quickstart (committed). |
| `data/raw_drop/` | Raw file drop (gitignored). |
| `dbt/` | dbt project; copy `profiles.template.yml` to `profiles.yml`. |
| `ingestion/` | Bronze ingest (CSV/JSON to Parquet or BigQuery). |
| `transform/` | Silver build (standardize and quarantine). |
| `scripts/` | `mvp_local.ps1`, `run_bigquery_gold.ps1`, `preflight_repo_audit.ps1`, and related helpers. |

---

## What This Project Demonstrates

This repository is intended as portfolio evidence of:

- **Healthcare analytics engineering** — End-to-end pipeline from raw hospital transparency files to comparison-ready marts and a live dashboard.
- **dbt dimensional modeling** — Staging, intermediate, and marts with clear grain; star-style dimensions and facts; seeds for mapping; and tests for relationships and guardrails.
- **BigQuery warehouse design** — Raw landing, staging, and marts in BigQuery with appropriate schemas and incremental patterns where used.
- **Data quality guardrails** — Comparability logic, rejects model, and tests that prevent invalid categories (e.g. “other”) from appearing in comparison outputs.
- **Semantic modeling** — A dedicated semantic fact that normalizes rate categories and exposes a single, BI-safe entry point for downstream comparison and reporting.
- **Dashboard and analytics app development** — A multi-page Streamlit app with filters, tables, charts, and downloads, wired to BigQuery and suitable for deployment on Streamlit Cloud.
