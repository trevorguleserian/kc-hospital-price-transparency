# Architecture

This document describes the end-to-end pipeline and main design decisions for the KC Hospital Price Transparency project. The system ingests heterogeneous hospital charge files, normalizes them in a lakehouse, transforms them with dbt into a star schema, and serves a Streamlit app for search, comparison, and data-quality review.

---

## Pipeline overview

Data flows through four stages: **Bronze** (raw landing), **Silver** (standardized, with quarantine), **dbt Gold** (star schema), and **Streamlit** (BI and discovery).

```
    +------------------+
    |  Raw sources     |
    |  local: data/    |
    |    raw_drop/     |
    |  gcs: bucket/    |
    |    pt_landing/   |
    +--------+---------+
             |
             v
  +------------------------------+
  |  BRONZE (Parquet)             |
  |  lake/bronze/pt_csv/          |
  |  lake/bronze/pt_json/         |
  |  ingest_date=YYYY-MM-DD/      |
  |  + file_manifest (DuckDB)     |
  +--------------+---------------+
             |
             v
  +------------------------------+
  |  SILVER (Parquet)             |
  |  standard_charges/            |
  |  quarantine/ (reason_code)    |
  |  ingest_date=YYYY-MM-DD/     |
  +--------------+---------------+
             |
     +-------+--------+
     |  DQ gate       |
     |  quarantine_  |
     |  rate < limit |
     +-------+--------+
             | pass
             v
  +------------------------------+
  |  GOLD (dbt)                   |
  |  local: DuckDB                |
  |  cloud: BigQuery              |
  |  staging -> intermediate      |
  |  -> marts (dims + fact)       |
  +--------------+---------------+
             |
             v
  +------------------------------+
  |  dbt tests                    |
  |  not_null, relationships,    |
  |  accepted_values              |
  +--------------+---------------+
             |
             v
  +------------------------------+
  |  BI exports (local)           |
  |  Parquet/CSV in dbt/exports/  |
  +--------------+---------------+
             |
             v
  +------------------------------+
  |  STREAMLIT APP                |
  |  Home | Search & Compare |     |
  |  Hospital Profile | Data Q    |
  +------------------------------+
```

---

## Stage summary

| Stage      | Purpose | Output |
|-----------|---------|--------|
| **Bronze** | Ingest raw CSV/JSON as-is; idempotent by file hash | Parquet under `lake/bronze/pt_csv/`, `lake/bronze/pt_json/`; manifest in DuckDB (local) |
| **Silver** | One row per rate; canonical columns; invalid rows quarantined | `lake/silver/standard_charges/`, `lake/silver/quarantine/` |
| **dbt Gold** | Staging from Silver or BigQuery → intermediate → marts | Star schema: `fct_standard_charges_semantic`, `dim_hospital`, `dim_payer`, `dim_procedure` |
| **Streamlit** | Search, compare, hospital profile, data-quality views | App reads from `dbt/exports/` (local) or BigQuery marts |

---

## Key design choices

- **Dual backend:** Local development uses DuckDB and Parquet under `lake/`; production uses BigQuery. dbt targets (`local_duckdb`, `bigquery`) and execution mode (`local` vs `bq`) switch sources and destination without changing model logic.
- **Bronze idempotency:** Ingest is keyed by file hash; already-processed files are skipped. Re-ingest is safe and supports backfills.
- **Silver quarantine:** Rows that fail validation (e.g. missing code or rate) are written to `quarantine/` with a `reason_code` instead of being dropped. A DQ gate (e.g. quarantine rate &lt; threshold) can fail the pipeline before Gold.
- **Single semantic fact:** The BI-facing table is `fct_standard_charges_semantic`: one row per (charge, rate_category) with normalized rate types (negotiated, gross, cash, etc.) and null `billing_code` filtered out so joins and tests are consistent.
- **Billing code type inference:** When the source does not provide a type (or it is UNKNOWN), the procedure dimension infers CPT/HCPCS/NDC/REVENUE/ICD-10-PCS from the code pattern; otherwise the source type is preserved.
- **Streamlit mode switch:** The app runs in `local` (Parquet/CSV from `dbt/exports/`) or `bigquery` via `APP_MODE`; the same UI works for both.

---

## Backends

| Mode  | Execution | Storage   | Gold DB   |
|-------|-----------|-----------|-----------|
| Cloud | `bq`      | GCS       | BigQuery  |
| Local | `local`   | local fs  | DuckDB    |

---

## Backfills and schema

- **Bronze:** Re-run ingest for a partition date; idempotent by `file_hash` (skip if SUCCESS in manifest).
- **Silver:** Re-run Silver build for that date; overwrites that partition’s Parquet.
- **Gold:** Full refresh or incremental via dbt; no date partition in dbt by default.
- **Schema:** Bronze adds `source_path`, `file_hash`, `ingest_date`. Silver uses canonical columns aligned with dbt; bad rows keep a `reason_code`. Gold is defined by dbt models and tests; schema changes require updates in both dbt and Silver column mapping.

For runbooks and failure handling, see [RUNBOOK.md](../RUNBOOK.md) and [docs/runbook.md](runbook.md).
