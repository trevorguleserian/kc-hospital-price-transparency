"""
Data access layer for Streamlit app.
BigQuery-only for Cloud runs; no local/demo mode.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).resolve().parent.parent.parent.parent / ".env"
    load_dotenv(_env_path)
except ImportError:
    pass

# Repo root: apps/streamlit_app/lib/data.py -> 4 levels up
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent

# Canonical table / file names (no extension for BQ; .parquet for local)
TABLE_NAMES = ("dim_hospital", "dim_payer", "dim_procedure", "fct_standard_charges_semantic")


def get_mode() -> str:
    """Effective mode: always 'bigquery' (Cloud / BigQuery-only app)."""
    return "bigquery"


def get_tables() -> tuple[str, ...]:
    """Return canonical table names."""
    return TABLE_NAMES


def get_active_source_label() -> str:
    """Human-readable active source for sidebar (project.dataset when BQ configured)."""
    return f"{_bq_project()}.{_bq_dataset()}"


def _local_export_dir() -> Path:
    """Path to Parquet exports (repo-relative from env)."""
    rel = os.environ.get("LOCAL_EXPORT_DIR", "dbt/exports").strip()
    return (_REPO_ROOT / rel).resolve()


def _bq_project() -> str:
    from lib import bq_auth
    project, _dataset, _loc, _ = bq_auth.get_bq_config()
    return project or os.environ.get("BQ_PROJECT", "pricing-transparency-portfolio").strip()


def _bq_dataset() -> str:
    """Marts dataset for BigQuery (dim_*, fct_*). Default pt_analytics_marts."""
    from lib import bq_auth
    _project, dataset_marts, _loc, _ = bq_auth.get_bq_config()
    return dataset_marts or os.environ.get("BQ_DATASET_MARTS") or os.environ.get("BQ_DATASET") or "pt_analytics_marts"


def _bq_table(table: str) -> str:
    """Fully qualified table identifier for BigQuery."""
    p, d = _bq_project(), _bq_dataset()
    return f"`{p}.{d}.{table}`"


# ---------------------------------------------------------------------------
# BigQuery
# ---------------------------------------------------------------------------

def _bq_client():
    from lib import bq_auth
    client, err = bq_auth.get_bq_client()
    if err:
        raise RuntimeError(err)
    return client


def _bq_query(sql: str, params: Optional[list] = None) -> pd.DataFrame:
    from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter
    client = _bq_client()
    job_config = QueryJobConfig(query_parameters=params or [])
    return client.query(sql, job_config=job_config).to_dataframe()


# ---------------------------------------------------------------------------
# Local (Parquet)
# ---------------------------------------------------------------------------

def _local_path(table: str, ext: str = "parquet") -> Path:
    """Path for a canonical table name (parquet or csv)."""
    return _local_export_dir() / f"{table}.{ext}"


def _read_local_table(table: str) -> pd.DataFrame:
    """Read from dbt/exports: parquet preferred, csv fallback."""
    p = _local_path(table, "parquet")
    if p.exists():
        return pd.read_parquet(p)
    c = _local_path(table, "csv")
    if c.exists():
        return pd.read_csv(c)
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# Public API (mode-agnostic)
# ---------------------------------------------------------------------------

@st.cache_data
def load_dim_hospital(_mode: Optional[str] = None) -> pd.DataFrame:
    """Load dim_hospital from BigQuery; cached."""
    sql = f"SELECT * FROM {_bq_table('dim_hospital')}"
    return _bq_query(sql)


@st.cache_data
def load_dim_payer(_mode: Optional[str] = None) -> pd.DataFrame:
    """Load dim_payer from BigQuery; cached."""
    sql = f"SELECT * FROM {_bq_table('dim_payer')}"
    return _bq_query(sql)


def search_procedures(query: str, limit: int = 50) -> pd.DataFrame:
    """Search dim_procedure by billing_code or description; return up to limit rows."""
    q = (query or "").strip()
    if not q:
        sql = f"SELECT billing_code, billing_code_type, description FROM {_bq_table('dim_procedure')} LIMIT {int(limit)}"
        return _bq_query(sql)
    q_like = f"%{q}%"
    sql = f"""
    SELECT billing_code, billing_code_type, description
    FROM {_bq_table('dim_procedure')}
    WHERE LOWER(COALESCE(billing_code, '')) LIKE LOWER(@q)
       OR LOWER(COALESCE(description, '')) LIKE LOWER(@q)
    LIMIT {int(limit)}
    """
    from google.cloud.bigquery import ScalarQueryParameter
    params = [ScalarQueryParameter("q", "STRING", q_like)]
    return _bq_query(sql, params)


def get_rates(
    hospital_id: str,
    billing_code: Optional[str] = None,
    payer_name: Optional[str] = None,
    plan_name: Optional[str] = None,
    rate_category: Optional[str] = None,
    billing_code_type: Optional[str] = None,
    limit: int = 500,
) -> pd.DataFrame:
    """Get fact rows filtered by hospital_id and optional filters from BigQuery."""
    limit = max(1, min(int(limit), 5000))
    from google.cloud.bigquery import ScalarQueryParameter
    conditions = ["hospital_id = @hospital_id"]
    params = [ScalarQueryParameter("hospital_id", "STRING", hospital_id)]
    if billing_code:
        conditions.append("billing_code = @billing_code")
        params.append(ScalarQueryParameter("billing_code", "STRING", billing_code))
    if payer_name:
        conditions.append("payer_name = @payer_name")
        params.append(ScalarQueryParameter("payer_name", "STRING", payer_name))
    if plan_name:
        conditions.append("plan_name = @plan_name")
        params.append(ScalarQueryParameter("plan_name", "STRING", plan_name))
    if rate_category:
        conditions.append("rate_category = @rate_category")
        params.append(ScalarQueryParameter("rate_category", "STRING", rate_category))
    if billing_code_type:
        conditions.append("billing_code_type = @billing_code_type")
        params.append(ScalarQueryParameter("billing_code_type", "STRING", billing_code_type))
    where = " AND ".join(conditions)
    sql = f"""
    SELECT semantic_charge_sk, hospital_id, hospital_name, billing_code, billing_code_type, description,
           payer_name, plan_name, rate_category, rate_amount, rate_unit, ingested_at
    FROM {_bq_table('fct_standard_charges_semantic')}
    WHERE {where}
    ORDER BY rate_amount DESC
    LIMIT {limit}
    """
    return _bq_query(sql, params)


@st.cache_data
def get_overview_metrics(_mode: Optional[str] = None) -> dict:
    """Counts and min/max ingested_at for fact and dims from BigQuery. Cached."""
    out = {
        "charges_rows": 0,
        "hospitals_rows": 0,
        "payers_rows": 0,
        "procedures_rows": 0,
        "min_ingested_at": None,
        "max_ingested_at": None,
    }
    sql_fact = f"""
    SELECT COUNT(*) AS n, MIN(ingested_at) AS min_at, MAX(ingested_at) AS max_at
    FROM {_bq_table('fct_standard_charges_semantic')}
    """
    row = _bq_query(sql_fact).iloc[0]
    out["charges_rows"] = int(row["n"] or 0)
    out["min_ingested_at"] = row.get("min_at")
    out["max_ingested_at"] = row.get("max_at")
    for table, key in (
        ("dim_hospital", "hospitals_rows"),
        ("dim_payer", "payers_rows"),
        ("dim_procedure", "procedures_rows"),
    ):
        r = _bq_query(f"SELECT COUNT(*) AS n FROM {_bq_table(table)}").iloc[0]
        out[key] = int(r["n"] or 0)
    return out


@st.cache_data
def get_rate_category_distribution(_mode: Optional[str] = None) -> pd.DataFrame:
    """Rate category counts for overview from BigQuery. Cached."""
    sql = f"""
    SELECT rate_category, COUNT(*) AS cnt
    FROM {_bq_table('fct_standard_charges_semantic')}
    GROUP BY rate_category
    ORDER BY cnt DESC
    """
    return _bq_query(sql)


def get_hospital_kpis(hospital_id: str) -> dict:
    """KPIs for one hospital from BigQuery: total rows, distinct billing_code, distinct payer/plan, median/min/max rate."""
    from google.cloud.bigquery import ScalarQueryParameter
    sql = f"""
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT billing_code) AS distinct_procedures,
        COUNT(DISTINCT CONCAT(COALESCE(payer_name,''), '|', COALESCE(plan_name,''))) AS distinct_payer_plan,
        APPROX_QUANTILES(rate_amount, 100)[OFFSET(50)] AS median_rate,
        MIN(rate_amount) AS min_rate,
        MAX(rate_amount) AS max_rate
    FROM {_bq_table('fct_standard_charges_semantic')}
    WHERE hospital_id = @hospital_id
    """
    params = [ScalarQueryParameter("hospital_id", "STRING", hospital_id)]
    row = _bq_query(sql, params).iloc[0]
    return {
        "total_rows": int(row["total_rows"] or 0),
        "distinct_procedures": int(row["distinct_procedures"] or 0),
        "distinct_payer_plan": int(row["distinct_payer_plan"] or 0),
        "median_rate": float(row["median_rate"]) if row.get("median_rate") is not None else None,
        "min_rate": float(row["min_rate"]) if row.get("min_rate") is not None else None,
        "max_rate": float(row["max_rate"]) if row.get("max_rate") is not None else None,
    }


def get_hospital_payer_coverage(hospital_id: str, limit: int = 100) -> pd.DataFrame:
    """Payer/plan row counts for one hospital from BigQuery."""
    limit = max(1, min(int(limit), 500))
    from google.cloud.bigquery import ScalarQueryParameter
    sql = f"""
    SELECT payer_name, plan_name, COUNT(*) AS row_count
    FROM {_bq_table('fct_standard_charges_semantic')}
    WHERE hospital_id = @hospital_id
    GROUP BY payer_name, plan_name
    ORDER BY row_count DESC
    LIMIT {limit}
    """
    return _bq_query(sql, [ScalarQueryParameter("hospital_id", "STRING", hospital_id)])


def get_hospital_top_procedures(hospital_id: str, limit: int = 50) -> pd.DataFrame:
    """Top procedures by row count for one hospital from BigQuery."""
    limit = max(1, min(int(limit), 200))
    from google.cloud.bigquery import ScalarQueryParameter
    sql = f"""
    SELECT billing_code, billing_code_type, description, COUNT(*) AS row_count
    FROM {_bq_table('fct_standard_charges_semantic')}
    WHERE hospital_id = @hospital_id
    GROUP BY billing_code, billing_code_type, description
    ORDER BY row_count DESC
    LIMIT {limit}
    """
    return _bq_query(sql, [ScalarQueryParameter("hospital_id", "STRING", hospital_id)])


@st.cache_data
def get_data_quality_metrics(_mode: Optional[str] = None) -> dict:
    """Null rates and UNKNOWN billing_code_type count from BigQuery. Cached."""
    out = {
        "pct_null_hospital_id": 0.0,
        "pct_null_billing_code": 0.0,
        "pct_null_rate_amount": 0.0,
        "pct_null_description": 0.0,
        "unknown_billing_code_type_count": 0,
        "total_rows": 0,
    }
    sql = f"""
    SELECT
        COUNT(*) AS n,
        COUNTIF(hospital_id IS NULL) AS null_hid,
        COUNTIF(billing_code IS NULL) AS null_bc,
        COUNTIF(rate_amount IS NULL) AS null_amt,
        COUNTIF(description IS NULL) AS null_desc,
        COUNTIF(LOWER(COALESCE(billing_code_type, '')) = 'unknown') AS unknown_bc_type
    FROM {_bq_table('fct_standard_charges_semantic')}
    """
    row = _bq_query(sql).iloc[0]
    n = int(row["n"] or 0)
    out["total_rows"] = n
    if n:
        out["pct_null_hospital_id"] = round(100.0 * int(row["null_hid"] or 0) / n, 2)
        out["pct_null_billing_code"] = round(100.0 * int(row["null_bc"] or 0) / n, 2)
        out["pct_null_rate_amount"] = round(100.0 * int(row["null_amt"] or 0) / n, 2)
        out["pct_null_description"] = round(100.0 * int(row["null_desc"] or 0) / n, 2)
        out["unknown_billing_code_type_count"] = int(row["unknown_bc_type"] or 0)
    return out


def get_outlier_rates(limit: int = 100) -> pd.DataFrame:
    """Top rate_amount rows (outliers) from BigQuery. Capped query."""
    limit = max(1, min(int(limit), 500))
    sql = f"""
    SELECT hospital_id, hospital_name, billing_code, description, rate_category, rate_amount, payer_name, plan_name
    FROM {_bq_table('fct_standard_charges_semantic')}
    ORDER BY rate_amount DESC
    LIMIT {limit}
    """
    return _bq_query(sql)


def ensure_data_available() -> tuple[bool, str]:
    """Check data availability. BigQuery-only: returns (ok, message). Stops early if secrets missing (no local path attempt)."""
    from lib import debug
    ok_secrets, missing_keys = debug.has_bq_secrets()
    if not ok_secrets:
        present = debug.secrets_keys()
        msg_lines = [
            "BigQuery selected but secrets are missing or incomplete.",
            "Present secrets keys: " + (", ".join(present) if present else "(none)"),
            "Missing keys: " + ", ".join(missing_keys),
            "Expected TOML structure: [gcp_service_account] with type, project_id, private_key_id, private_key, client_email; and BQ_PROJECT, BQ_LOCATION, BQ_DATASET_MARTS.",
        ]
        return False, "\n".join(msg_lines)
    from lib import bq_auth
    client, err = bq_auth.get_bq_client()
    if err or client is None:
        present = debug.secrets_keys()
        return False, (
            "BigQuery client creation failed. "
            "Present secrets keys: " + (", ".join(present) if present else "(none)") + ". "
            + (err or "Unknown error.")
        )
    project_id, dataset, _loc, _ = bq_auth.get_bq_config()
    if not project_id or not dataset:
        return False, "BigQuery not configured: set BQ_PROJECT and BQ_DATASET_MARTS in Streamlit secrets or env."
    try:
        sql = f"SELECT 1 FROM {_bq_table('dim_hospital')} LIMIT 1"
        client.query(sql).result()
        return True, ""
    except Exception as e:
        return False, f"BigQuery unavailable: {e}"


def get_local_exports_instructions() -> str:
    """Friendly markdown with steps to generate dbt/exports for demo mode (no credentials)."""
    return """
**Demo mode** uses data from `dbt/exports/` (Parquet or CSV). To generate them:

1. **Bronze ingest** — Put raw CSV/JSON in `data/raw_drop/`, then:
   ```powershell
   $env:STORAGE_BACKEND = "local"; $env:PYTHONPATH = (Get-Location).Path
   python -c "from ingestion.bronze_ingest import run_bronze_ingest; run_bronze_ingest(ingest_date='YYYY-MM-DD')"
   ```
2. **Silver build** — `python -c "from transform.silver_build import build_silver_for_date; build_silver_for_date('YYYY-MM-DD', base_dir='.')"`
3. **dbt build + export** — Set `DBT_SILVER_GLOB` to your Silver Parquet glob, then run `scripts/run_local_bi.ps1` (or `dbt build` + `dbt run-operation export_bi_outputs` in `dbt/`).

**Shortcut:** If Silver already exists, run `scripts/mvp_local.ps1` to build and export, then start Streamlit again.
"""


