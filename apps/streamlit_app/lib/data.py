"""
Data access layer for Streamlit app.
Supports APP_MODE=bigquery (BigQuery) or APP_MODE=local (Parquet from LOCAL_EXPORT_DIR).
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
    """Effective mode: session state (UI) overrides env APP_MODE. Default 'local' (no creds)."""
    return (st.session_state.get("app_data_source") or os.environ.get("APP_MODE") or "local").strip().lower()


def get_tables() -> tuple[str, ...]:
    """Return canonical table names."""
    return TABLE_NAMES


def get_active_source_label() -> str:
    """Human-readable active source for sidebar (e.g. project.dataset or export path)."""
    if get_mode() == "bigquery":
        return f"{_bq_project()}.{_bq_dataset()}"
    return str(_local_export_dir())


def _local_export_dir() -> Path:
    """Path to Parquet exports (repo-relative from env)."""
    rel = os.environ.get("LOCAL_EXPORT_DIR", "dbt/exports").strip()
    return (_REPO_ROOT / rel).resolve()


def _bq_project() -> str:
    return os.environ.get("BQ_PROJECT", "pricing-transparency-portfolio").strip()


def _bq_dataset() -> str:
    return os.environ.get("BQ_DATASET", "pt_analytics_marts").strip()


def _bq_table(table: str) -> str:
    """Fully qualified table identifier for BigQuery."""
    p, d = _bq_project(), _bq_dataset()
    return f"`{p}.{d}.{table}`"


# ---------------------------------------------------------------------------
# BigQuery
# ---------------------------------------------------------------------------

def _bq_client():
    from google.cloud import bigquery
    return bigquery.Client()


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
    """Load dim_hospital; cached by data source mode."""
    mode = _mode or get_mode()
    if mode == "bigquery":
        sql = f"SELECT * FROM {_bq_table('dim_hospital')}"
        return _bq_query(sql)
    # local
    return _read_local_table("dim_hospital")


@st.cache_data
def load_dim_payer(_mode: Optional[str] = None) -> pd.DataFrame:
    """Load dim_payer; cached by data source mode."""
    mode = _mode or get_mode()
    if mode == "bigquery":
        sql = f"SELECT * FROM {_bq_table('dim_payer')}"
        return _bq_query(sql)
    return _read_local_table("dim_payer")


def search_procedures(query: str, limit: int = 50) -> pd.DataFrame:
    """Search dim_procedure by billing_code or description; return up to limit rows."""
    mode = get_mode()
    q = (query or "").strip()
    if not q:
        if mode == "bigquery":
            sql = f"SELECT billing_code, billing_code_type, description FROM {_bq_table('dim_procedure')} LIMIT {int(limit)}"
            return _bq_query(sql)
        df = _read_local_table("dim_procedure")
        return df.head(limit) if not df.empty else df

    q_like = f"%{q}%"
    if mode == "bigquery":
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

    df = _read_local_table("dim_procedure")
    if df.empty:
        return df
    q_lower = q.lower()
    mask = (
        df["billing_code"].astype(str).str.lower().str.contains(q_lower, na=False)
        | df["description"].astype(str).str.lower().str.contains(q_lower, na=False)
    )
    return df.loc[mask].head(limit)


def get_rates(
    hospital_id: str,
    billing_code: Optional[str] = None,
    payer_name: Optional[str] = None,
    plan_name: Optional[str] = None,
    rate_category: Optional[str] = None,
    billing_code_type: Optional[str] = None,
    limit: int = 500,
) -> pd.DataFrame:
    """Get fact rows filtered by hospital_id and optional filters. Never load full fact in BQ."""
    mode = get_mode()
    limit = max(1, min(int(limit), 5000))

    if mode == "bigquery":
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

    df = _read_local_table("fct_standard_charges_semantic")
    if df.empty:
        return df
    df = df.loc[df["hospital_id"].astype(str) == str(hospital_id)]
    if billing_code:
        df = df.loc[df["billing_code"].astype(str) == str(billing_code)]
    if payer_name:
        df = df.loc[df["payer_name"].astype(str) == str(payer_name)]
    if plan_name:
        df = df.loc[df["plan_name"].astype(str) == str(plan_name)]
    if rate_category:
        df = df.loc[df["rate_category"].astype(str) == str(rate_category)]
    if billing_code_type:
        df = df.loc[df["billing_code_type"].astype(str) == str(billing_code_type)]
    cols = [c for c in ["semantic_charge_sk", "hospital_id", "hospital_name", "billing_code", "billing_code_type",
                        "description", "payer_name", "plan_name", "rate_category", "rate_amount", "rate_unit", "ingested_at"] if c in df.columns]
    return df[cols].sort_values("rate_amount", ascending=False).head(limit)


@st.cache_data
def get_overview_metrics(_mode: Optional[str] = None) -> dict:
    """Counts and min/max ingested_at for fact and dims. Cached by data source mode."""
    mode = _mode or get_mode()
    out = {
        "charges_rows": 0,
        "hospitals_rows": 0,
        "payers_rows": 0,
        "procedures_rows": 0,
        "min_ingested_at": None,
        "max_ingested_at": None,
    }

    if mode == "bigquery":
        project, dataset = _bq_project(), _bq_dataset()
        # Single query for fact counts + min/max
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

    # local
    for table, key in (
        ("fct_standard_charges_semantic", "charges_rows"),
        ("dim_hospital", "hospitals_rows"),
        ("dim_payer", "payers_rows"),
        ("dim_procedure", "procedures_rows"),
    ):
        df = _read_local_table(table)
        if df.empty:
            continue
        n = len(df)
        out[key] = n
        if table == "fct_standard_charges_semantic" and n and "ingested_at" in df.columns:
            out["min_ingested_at"] = df["ingested_at"].min()
            out["max_ingested_at"] = df["ingested_at"].max()
    return out


@st.cache_data
def get_rate_category_distribution(_mode: Optional[str] = None) -> pd.DataFrame:
    """Rate category counts for overview. Cached by data source mode."""
    mode = _mode or get_mode()
    if mode == "bigquery":
        sql = f"""
        SELECT rate_category, COUNT(*) AS cnt
        FROM {_bq_table('fct_standard_charges_semantic')}
        GROUP BY rate_category
        ORDER BY cnt DESC
        """
        return _bq_query(sql)
    df = _read_local_table("fct_standard_charges_semantic")
    if df.empty or "rate_category" not in df.columns:
        return pd.DataFrame(columns=["rate_category", "cnt"])
    out = df["rate_category"].value_counts().reset_index()
    out.columns = ["rate_category", "cnt"]
    return out


def get_hospital_kpis(hospital_id: str) -> dict:
    """KPIs for one hospital: total rows, distinct billing_code, distinct payer/plan, median/min/max rate."""
    mode = get_mode()
    if mode == "bigquery":
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
    df = _read_local_table("fct_standard_charges_semantic")
    if df.empty:
        return {"total_rows": 0, "distinct_procedures": 0, "distinct_payer_plan": 0, "median_rate": None, "min_rate": None, "max_rate": None}
    sub = df.loc[df["hospital_id"].astype(str) == str(hospital_id)]
    if sub.empty:
        return {"total_rows": 0, "distinct_procedures": 0, "distinct_payer_plan": 0, "median_rate": None, "min_rate": None, "max_rate": None}
    rate = sub["rate_amount"].astype(float, errors="coerce").dropna()
    return {
        "total_rows": len(sub),
        "distinct_procedures": sub["billing_code"].nunique(),
        "distinct_payer_plan": sub.apply(lambda r: f"{r.get('payer_name') or ''}|{r.get('plan_name') or ''}", axis=1).nunique(),
        "median_rate": float(rate.median()) if len(rate) else None,
        "min_rate": float(rate.min()) if len(rate) else None,
        "max_rate": float(rate.max()) if len(rate) else None,
    }


def get_hospital_payer_coverage(hospital_id: str, limit: int = 100) -> pd.DataFrame:
    """Payer/plan row counts for one hospital."""
    limit = max(1, min(int(limit), 500))
    mode = get_mode()
    if mode == "bigquery":
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
    df = _read_local_table("fct_standard_charges_semantic")
    if df.empty:
        return pd.DataFrame(columns=["payer_name", "plan_name", "row_count"])
    sub = df.loc[df["hospital_id"].astype(str) == str(hospital_id)]
    out = sub.groupby(["payer_name", "plan_name"], dropna=False).size().reset_index(name="row_count")
    return out.sort_values("row_count", ascending=False).head(limit)


def get_hospital_top_procedures(hospital_id: str, limit: int = 50) -> pd.DataFrame:
    """Top procedures by row count for one hospital."""
    limit = max(1, min(int(limit), 200))
    mode = get_mode()
    if mode == "bigquery":
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
    df = _read_local_table("fct_standard_charges_semantic")
    if df.empty:
        return pd.DataFrame(columns=["billing_code", "billing_code_type", "description", "row_count"])
    sub = df.loc[df["hospital_id"].astype(str) == str(hospital_id)]
    grp = sub.groupby(["billing_code", "billing_code_type", "description"], dropna=False).size().reset_index(name="row_count")
    return grp.sort_values("row_count", ascending=False).head(limit)


@st.cache_data
def get_data_quality_metrics(_mode: Optional[str] = None) -> dict:
    """Null rates and UNKNOWN billing_code_type count. Cached by data source mode."""
    mode = _mode or get_mode()
    out = {
        "pct_null_hospital_id": 0.0,
        "pct_null_billing_code": 0.0,
        "pct_null_rate_amount": 0.0,
        "pct_null_description": 0.0,
        "unknown_billing_code_type_count": 0,
        "total_rows": 0,
    }
    if mode == "bigquery":
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
    df = _read_local_table("fct_standard_charges_semantic")
    if df.empty:
        return out
    n = len(df)
    out["total_rows"] = n
    for col, key in (("hospital_id", "pct_null_hospital_id"), ("billing_code", "pct_null_billing_code"), ("rate_amount", "pct_null_rate_amount"), ("description", "pct_null_description")):
        if col in df.columns:
            out[key] = round(100.0 * df[col].isna().sum() / n, 2)
    if "billing_code_type" in df.columns:
        out["unknown_billing_code_type_count"] = int(df["billing_code_type"].astype(str).str.lower().eq("unknown").sum())
    return out


def get_outlier_rates(limit: int = 100) -> pd.DataFrame:
    """Top rate_amount rows (outliers). Capped query."""
    limit = max(1, min(int(limit), 500))
    mode = get_mode()
    if mode == "bigquery":
        sql = f"""
        SELECT hospital_id, hospital_name, billing_code, description, rate_category, rate_amount, payer_name, plan_name
        FROM {_bq_table('fct_standard_charges_semantic')}
        ORDER BY rate_amount DESC
        LIMIT {limit}
        """
        return _bq_query(sql)
    df = _read_local_table("fct_standard_charges_semantic")
    if df.empty:
        return pd.DataFrame()
    cols = [c for c in ["hospital_id", "hospital_name", "billing_code", "description", "rate_category", "rate_amount", "payer_name", "plan_name"] if c in df.columns]
    return df[cols].sort_values("rate_amount", ascending=False).head(limit)


def ensure_data_available() -> tuple[bool, str]:
    """Check data availability. Returns (ok, message). When local + SAMPLE_DATA=1, auto-bootstrap if exports missing."""
    mode = get_mode()
    if mode == "local":
        try:
            from lib import bootstrap
            bootstrap.ensure_demo_exports_exist(st)
        except Exception:
            pass  # proceed to missing check; bootstrap may have failed
    if mode == "bigquery":
        try:
            from google.cloud import bigquery
            client = bigquery.Client()
            sql = f"SELECT 1 FROM {_bq_table('dim_hospital')} LIMIT 1"
            client.query(sql).result()
            return True, ""
        except Exception as e:
            return False, f"BigQuery unavailable: {e}. Set GOOGLE_APPLICATION_CREDENTIALS or use gcloud auth application-default login."
    export_dir = _local_export_dir()
    required = ["dim_hospital", "fct_standard_charges_semantic"]
    missing = [t for t in required if not (export_dir / f"{t}.parquet").exists() and not (export_dir / f"{t}.csv").exists()]
    if missing:
        return False, f"Local data missing in {export_dir}: {', '.join(missing)} (need .parquet or .csv). Run scripts/run_local_bi.ps1 or scripts/mvp_local.ps1 first."
    return True, ""


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


