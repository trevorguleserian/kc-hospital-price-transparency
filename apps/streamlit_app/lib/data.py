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
    """Load dim_hospital from BigQuery; cached. Adds hospital_display_name = hospital_name_clean for UI."""
    sql = f"SELECT * FROM {_bq_table('dim_hospital')}"
    df = _bq_query(sql)
    if not df.empty and "hospital_name_clean" in df.columns:
        df = df.copy()
        df["hospital_display_name"] = df["hospital_name_clean"]
    return df


@st.cache_data
def load_dim_payer(_mode: Optional[str] = None) -> pd.DataFrame:
    """Load dim_payer from BigQuery; cached."""
    sql = f"SELECT * FROM {_bq_table('dim_payer')}"
    return _bq_query(sql)


@st.cache_data
def load_dim_payer_harmonized(_mode: Optional[str] = None) -> pd.DataFrame:
    """Load dim_payer_harmonized (payer_family, plan_family) from BigQuery; cached."""
    sql = f"SELECT payer_family, plan_family FROM {_bq_table('dim_payer_harmonized')}"
    return _bq_query(sql)


@st.cache_data
def get_hospital_comparison(
    billing_code: str,
    payer_family: str,
    rate_category: str,
    rate_unit: str,
    plan_family: Optional[str] = None,
    billing_code_type: Optional[str] = None,
    hospital_ids: Optional[list[str]] = None,
) -> pd.DataFrame:
    """
    Query agg_hospital_procedure_compare joined to dim_hospital (hospital_name_clean) and
    dim_procedure_harmonized (canonical_description). Like-to-like: billing_code, rate_category,
    rate_unit, payer_family required; plan_family optional. All filters parameterized.
    """
    from google.cloud.bigquery import ArrayQueryParameter, QueryJobConfig, ScalarQueryParameter

    conditions = [
        "a.billing_code = @billing_code",
        "a.payer_family = @payer_family",
        "a.rate_category = @rate_category",
        "a.rate_unit = @rate_unit",
    ]
    params = [
        ScalarQueryParameter("billing_code", "STRING", (billing_code or "").strip()),
        ScalarQueryParameter("payer_family", "STRING", (payer_family or "").strip()),
        ScalarQueryParameter("rate_category", "STRING", (rate_category or "").strip()),
        ScalarQueryParameter("rate_unit", "STRING", (rate_unit or "").strip()),
    ]
    if plan_family is not None and str(plan_family).strip():
        conditions.append("a.plan_family = @plan_family")
        params.append(ScalarQueryParameter("plan_family", "STRING", str(plan_family).strip()))
    if billing_code_type and (str(billing_code_type).strip().upper() not in ("", "ANY", "— ANY —")):
        conditions.append("COALESCE(CAST(a.billing_code_type AS STRING), 'UNKNOWN') = @billing_code_type")
        params.append(ScalarQueryParameter("billing_code_type", "STRING", str(billing_code_type).strip()))
    if hospital_ids:
        conditions.append("a.hospital_id IN UNNEST(@hospital_ids)")
        params.append(ArrayQueryParameter("hospital_ids", "STRING", hospital_ids))

    where = " AND ".join(conditions)
    sql = f"""
    SELECT
      a.hospital_id,
      COALESCE(d.hospital_name_clean, a.hospital_id) AS hospital_name_clean,
      COALESCE(d.hospital_name_clean, a.hospital_id) AS hospital_display_name,
      a.billing_code,
      COALESCE(CAST(a.billing_code_type AS STRING), 'UNKNOWN') AS billing_code_type,
      COALESCE(proc.canonical_description, '') AS description,
      a.payer_family,
      a.plan_family,
      a.rate_category,
      a.rate_unit,
      a.min_rate,
      a.max_rate,
      a.approx_median_rate,
      a.row_count,
      COALESCE(proc.canonical_description, '') AS canonical_description
    FROM {_bq_table('agg_hospital_procedure_compare')} a
    LEFT JOIN {_bq_table('dim_hospital')} d ON a.hospital_id = d.hospital_id
    LEFT JOIN {_bq_table('dim_procedure_harmonized')} proc
      ON a.billing_code = proc.billing_code AND COALESCE(CAST(a.billing_code_type AS STRING), 'UNKNOWN') = proc.billing_code_type
    WHERE {where}
    ORDER BY a.approx_median_rate DESC
    """
    job_config = QueryJobConfig(query_parameters=params)
    client = _bq_client()
    return client.query(sql, job_config=job_config).to_dataframe()


@st.cache_data
def get_top_codes_by_type(
    billing_code_type: Optional[str] = None,
    hospital_ids: Optional[list[str]] = None,
    limit: int = 100,
) -> pd.DataFrame:
    """
    Top billing codes by row count from app-facing agg_hospital_procedure_compare (valid codes only).
    Optional filter by billing_code_type and hospital_ids. Returns billing_code, billing_code_type,
    canonical_description, row_count, hospitals_covered. Join to dim_procedure_harmonized for description.
    """
    from google.cloud.bigquery import ArrayQueryParameter, QueryJobConfig, ScalarQueryParameter

    limit = max(1, min(int(limit), 500))
    conditions = ["1 = 1"]
    params: list = []
    if billing_code_type and str(billing_code_type).strip():
        conditions.append("COALESCE(CAST(a.billing_code_type AS STRING), 'UNKNOWN') = @billing_code_type")
        params.append(ScalarQueryParameter("billing_code_type", "STRING", str(billing_code_type).strip()))
    if hospital_ids:
        conditions.append("a.hospital_id IN UNNEST(@hospital_ids)")
        params.append(ArrayQueryParameter("hospital_ids", "STRING", hospital_ids))

    where = " AND ".join(conditions)
    sql = f"""
    SELECT
      a.billing_code,
      COALESCE(CAST(a.billing_code_type AS STRING), 'UNKNOWN') AS billing_code_type,
      MAX(COALESCE(proc.canonical_description, '')) AS canonical_description,
      SUM(a.row_count) AS row_count,
      COUNT(DISTINCT a.hospital_id) AS hospitals_covered
    FROM {_bq_table('agg_hospital_procedure_compare')} a
    LEFT JOIN {_bq_table('dim_procedure_harmonized')} proc
      ON a.billing_code = proc.billing_code AND COALESCE(CAST(a.billing_code_type AS STRING), 'UNKNOWN') = proc.billing_code_type
    WHERE {where}
    GROUP BY a.billing_code, a.billing_code_type
    ORDER BY row_count DESC
    LIMIT {limit}
    """
    job_config = QueryJobConfig(query_parameters=params) if params else None
    client = _bq_client()
    return client.query(sql, job_config=job_config).to_dataframe()


@st.cache_data
def get_payer_plan_compare_detail(
    billing_code: str,
    rate_category: str,
    rate_unit: str,
    payer_family: Optional[str] = None,
    plan_family: Optional[str] = None,
    billing_code_type: Optional[str] = None,
    hospital_ids: Optional[list[str]] = None,
) -> pd.DataFrame:
    """
    Filtered rows from agg_payer_plan_compare for payer/plan comparison page. Optional
    payer_family/plan_family for like-to-like. Joins dim_procedure_harmonized for canonical_description.
    All filters parameterized; no full table scan.
    """
    from google.cloud.bigquery import ArrayQueryParameter, QueryJobConfig, ScalarQueryParameter

    conditions = [
        "a.billing_code = @billing_code",
        "a.rate_category = @rate_category",
        "a.rate_unit = @rate_unit",
    ]
    params = [
        ScalarQueryParameter("billing_code", "STRING", (billing_code or "").strip()),
        ScalarQueryParameter("rate_category", "STRING", (rate_category or "").strip()),
        ScalarQueryParameter("rate_unit", "STRING", (rate_unit or "").strip()),
    ]
    if payer_family is not None and str(payer_family).strip():
        conditions.append("a.payer_family = @payer_family")
        params.append(ScalarQueryParameter("payer_family", "STRING", str(payer_family).strip()))
    if plan_family is not None and str(plan_family).strip():
        conditions.append("a.plan_family = @plan_family")
        params.append(ScalarQueryParameter("plan_family", "STRING", str(plan_family).strip()))
    if billing_code_type and (str(billing_code_type).strip().upper() not in ("", "ANY", "— ANY —")):
        conditions.append("COALESCE(CAST(a.billing_code_type AS STRING), 'UNKNOWN') = @billing_code_type")
        params.append(ScalarQueryParameter("billing_code_type", "STRING", str(billing_code_type).strip()))
    if hospital_ids:
        conditions.append("a.hospital_id IN UNNEST(@hospital_ids)")
        params.append(ArrayQueryParameter("hospital_ids", "STRING", hospital_ids))

    where = " AND ".join(conditions)
    sql = f"""
    SELECT
      a.hospital_id,
      COALESCE(d.hospital_name_clean, a.hospital_id) AS hospital_name_clean,
      COALESCE(d.hospital_name_clean, a.hospital_id) AS hospital_display_name,
      a.billing_code,
      COALESCE(CAST(a.billing_code_type AS STRING), 'UNKNOWN') AS billing_code_type,
      COALESCE(proc.canonical_description, '') AS description,
      a.payer_family,
      a.plan_family,
      a.rate_category,
      a.rate_unit,
      a.min_rate,
      a.max_rate,
      a.approx_median_rate,
      a.row_count,
      COALESCE(proc.canonical_description, '') AS canonical_description
    FROM {_bq_table('agg_payer_plan_compare')} a
    LEFT JOIN {_bq_table('dim_hospital')} d ON a.hospital_id = d.hospital_id
    LEFT JOIN {_bq_table('dim_procedure_harmonized')} proc
      ON a.billing_code = proc.billing_code AND COALESCE(CAST(a.billing_code_type AS STRING), 'UNKNOWN') = proc.billing_code_type
    WHERE {where}
    """
    job_config = QueryJobConfig(query_parameters=params)
    client = _bq_client()
    return client.query(sql, job_config=job_config).to_dataframe()


def get_rejects_summary(
    billing_code: str,
    rate_category: str,
    rate_unit: str,
    limit_rows: int = 2000,
) -> pd.DataFrame:
    """
    Rejected rows (is_comparable = FALSE) for the given filters: aggregated by comparability_reason
    with row_count and optional sample. Reads from fct_rates_comparable_rejects; parameterized and capped.
    """
    from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter

    limit_rows = max(1, min(int(limit_rows), 2000))
    sql = f"""
    SELECT
      comparability_reason,
      COUNT(*) AS row_count,
      ANY_VALUE(payer_name) AS sample_payer_name,
      ANY_VALUE(hospital_id) AS sample_hospital_id
    FROM (
      SELECT comparability_reason, payer_name, hospital_id
      FROM {_bq_table('fct_rates_comparable_rejects')}
      WHERE billing_code = @billing_code
        AND rate_category = @rate_category
        AND rate_unit = @rate_unit
      LIMIT {limit_rows}
    ) limited
    GROUP BY comparability_reason
    ORDER BY row_count DESC
    """
    params = [
        ScalarQueryParameter("billing_code", "STRING", (billing_code or "").strip()),
        ScalarQueryParameter("rate_category", "STRING", (rate_category or "").strip()),
        ScalarQueryParameter("rate_unit", "STRING", (rate_unit or "").strip()),
    ]
    job_config = QueryJobConfig(query_parameters=params)
    client = _bq_client()
    return client.query(sql, job_config=job_config).to_dataframe()


def search_procedures(query: str, limit: int = 50) -> pd.DataFrame:
    """Search dim_procedure_harmonized by billing_code or canonical description; returns description = canonical_description for display."""
    q = (query or "").strip()
    if not q:
        sql = f"""
        SELECT billing_code, billing_code_type, canonical_description AS description
        FROM {_bq_table('dim_procedure_harmonized')}
        LIMIT {int(limit)}
        """
        return _bq_query(sql)
    q_like = f"%{q}%"
    sql = f"""
    SELECT billing_code, billing_code_type, canonical_description AS description
    FROM {_bq_table('dim_procedure_harmonized')}
    WHERE LOWER(COALESCE(billing_code, '')) LIKE LOWER(@q)
       OR LOWER(COALESCE(canonical_description, '')) LIKE LOWER(@q)
    LIMIT {int(limit)}
    """
    from google.cloud.bigquery import ScalarQueryParameter
    params = [ScalarQueryParameter("q", "STRING", q_like)]
    return _bq_query(sql, params)


def get_rates(
    hospital_id: Optional[str] = None,
    hospital_ids: Optional[list[str]] = None,
    billing_code: Optional[str] = None,
    payer_name: Optional[str] = None,
    plan_name: Optional[str] = None,
    payer_family: Optional[str] = None,
    plan_family: Optional[str] = None,
    rate_category: Optional[str] = None,
    rate_unit: Optional[str] = None,
    billing_code_type: Optional[str] = None,
    use_comparable: bool = True,
    limit: int = 500,
) -> pd.DataFrame:
    """
    Get rate rows from BigQuery. When use_comparable=True (default), query fct_rates_comparable_harmonized
    with required billing_code, rate_category, rate_unit; optional payer_family, plan_family, hospital filter.
    When use_comparable=False, query fct_standard_charges_semantic with required hospital_id (current behavior).
    Returns hospital_name_clean, description (canonical), payer_family, plan_family where available.
    """
    limit = max(1, min(int(limit), 5000))
    from google.cloud.bigquery import ArrayQueryParameter, QueryJobConfig, ScalarQueryParameter

    if use_comparable:
        # Required: billing_code, rate_category, rate_unit. Optional: payer_family, plan_family, billing_code_type, hospital_id or hospital_ids.
        if not billing_code or not rate_category or not rate_unit:
            return pd.DataFrame()
        conditions = [
            "f.billing_code = @billing_code",
            "f.rate_category = @rate_category",
            "f.rate_unit = @rate_unit",
        ]
        params = [
            ScalarQueryParameter("billing_code", "STRING", (billing_code or "").strip()),
            ScalarQueryParameter("rate_category", "STRING", rate_category.strip()),
            ScalarQueryParameter("rate_unit", "STRING", rate_unit.strip()),
        ]
        if billing_code_type and str(billing_code_type).strip().upper() not in ("", "ANY", "— ANY —"):
            conditions.append("COALESCE(CAST(f.billing_code_type AS STRING), 'UNKNOWN') = @billing_code_type")
            params.append(ScalarQueryParameter("billing_code_type", "STRING", str(billing_code_type).strip()))
        if payer_family is not None and str(payer_family).strip():
            conditions.append("f.payer_family = @payer_family")
            params.append(ScalarQueryParameter("payer_family", "STRING", str(payer_family).strip()))
        if plan_family is not None and str(plan_family).strip():
            conditions.append("f.plan_family = @plan_family")
            params.append(ScalarQueryParameter("plan_family", "STRING", str(plan_family).strip()))
        if hospital_ids:
            conditions.append("f.hospital_id IN UNNEST(@hospital_ids)")
            params.append(ArrayQueryParameter("hospital_ids", "STRING", hospital_ids))
        elif hospital_id and str(hospital_id).strip():
            conditions.append("f.hospital_id = @hospital_id")
            params.append(ScalarQueryParameter("hospital_id", "STRING", str(hospital_id).strip()))
        where = " AND ".join(conditions)
        sql = f"""
        SELECT f.semantic_charge_sk, f.hospital_id,
               COALESCE(d.hospital_name_clean, f.hospital_name) AS hospital_name_clean,
               f.billing_code, f.billing_code_type,
               COALESCE(proc.canonical_description, f.description) AS description,
               f.payer_family, f.plan_family,
               f.rate_category, f.rate_amount, f.rate_unit, f.ingested_at
        FROM {_bq_table('fct_rates_comparable_harmonized')} f
        LEFT JOIN {_bq_table('dim_hospital')} d ON f.hospital_id = d.hospital_id
        LEFT JOIN {_bq_table('dim_procedure_harmonized')} proc
          ON f.billing_code = proc.billing_code AND COALESCE(CAST(f.billing_code_type AS STRING), 'UNKNOWN') = proc.billing_code_type
        WHERE {where}
        ORDER BY f.rate_amount DESC
        LIMIT {limit}
        """
        job_config = QueryJobConfig(query_parameters=params)
        return _bq_client().query(sql, job_config=job_config).to_dataframe()
    else:
        # Semantic fact: hospital_id required; optional billing_code, payer_name, plan_name, rate_category, billing_code_type
        if not hospital_id or not str(hospital_id).strip():
            return pd.DataFrame()
        conditions = ["f.hospital_id = @hospital_id"]
        params = [ScalarQueryParameter("hospital_id", "STRING", str(hospital_id).strip())]
        if billing_code:
            conditions.append("f.billing_code = @billing_code")
            params.append(ScalarQueryParameter("billing_code", "STRING", billing_code))
        if payer_name:
            conditions.append("f.payer_name = @payer_name")
            params.append(ScalarQueryParameter("payer_name", "STRING", payer_name))
        if plan_name:
            conditions.append("f.plan_name = @plan_name")
            params.append(ScalarQueryParameter("plan_name", "STRING", plan_name))
        if rate_category:
            conditions.append("f.rate_category = @rate_category")
            params.append(ScalarQueryParameter("rate_category", "STRING", rate_category))
        if billing_code_type:
            conditions.append("COALESCE(CAST(f.billing_code_type AS STRING), 'UNKNOWN') = @billing_code_type")
            params.append(ScalarQueryParameter("billing_code_type", "STRING", str(billing_code_type).strip()))
        where = " AND ".join(conditions)
        sql = f"""
        SELECT f.semantic_charge_sk, f.hospital_id,
               COALESCE(d.hospital_name_clean, f.hospital_name) AS hospital_name_clean,
               f.billing_code, f.billing_code_type,
               COALESCE(proc.canonical_description, f.description) AS description,
               COALESCE(ph.payer_family, f.payer_name) AS payer_family,
               COALESCE(ph.plan_family, f.plan_name) AS plan_family,
               f.rate_category, f.rate_amount, f.rate_unit, f.ingested_at
        FROM {_bq_table('fct_standard_charges_semantic')} f
        LEFT JOIN {_bq_table('dim_hospital')} d ON f.hospital_id = d.hospital_id
        LEFT JOIN {_bq_table('dim_procedure_harmonized')} proc
          ON f.billing_code = proc.billing_code AND COALESCE(CAST(f.billing_code_type AS STRING), 'UNKNOWN') = proc.billing_code_type
        LEFT JOIN {_bq_table('dim_payer_harmonized')} ph
          ON COALESCE(TRIM(f.payer_name), '') = ph.payer_name_raw AND COALESCE(TRIM(f.plan_name), '') = ph.plan_name_raw
        WHERE {where}
        ORDER BY f.rate_amount DESC
        LIMIT {limit}
        """
        job_config = QueryJobConfig(query_parameters=params)
        return _bq_client().query(sql, job_config=job_config).to_dataframe()


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
def get_home_hospital_code_type_breakdown(
    exclude_types: Optional[list[str]] = None,
    _mode: Optional[str] = None,
) -> pd.DataFrame:
    """
    Distinct billing_code count by hospital (hospital_name_clean) and billing_code_type.
    Uses fct_standard_charges_semantic joined to dim_hospital. Excludes APC by default.
    Returns: hospital_name_clean, billing_code_type, distinct_codes, distinct_billing_code_count, total_distinct_codes.
    Sorted by total_distinct_codes DESC. exclude_types: billing_code_type values to exclude (default ['APC']).
    """
    exclude = list(exclude_types) if exclude_types is not None else ["APC"]
    exclude_upper = [x.strip().upper() for x in exclude if x and str(x).strip()]

    sql = f"""
    WITH base AS (
      SELECT
        COALESCE(d.hospital_name_clean, f.hospital_id) AS hospital_name_clean,
        COALESCE(TRIM(CAST(f.billing_code_type AS STRING)), 'UNKNOWN') AS billing_code_type,
        COUNT(DISTINCT f.billing_code) AS distinct_codes
      FROM {_bq_table('fct_standard_charges_semantic')} f
      LEFT JOIN {_bq_table('dim_hospital')} d ON f.hospital_id = d.hospital_id
      WHERE 1=1
    """
    if exclude_upper:
        placeholders = ", ".join([f"'{e}'" for e in exclude_upper])
        sql += f"\n      AND UPPER(TRIM(COALESCE(CAST(f.billing_code_type AS STRING), ''))) NOT IN ({placeholders})"
    sql += """
      GROUP BY 1, 2
    )
    SELECT
      hospital_name_clean,
      billing_code_type,
      distinct_codes,
      distinct_codes AS distinct_billing_code_count,
      SUM(distinct_codes) OVER (PARTITION BY hospital_name_clean) AS total_distinct_codes
    FROM base
    ORDER BY total_distinct_codes DESC, hospital_name_clean, billing_code_type
    """
    return _bq_query(sql)


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
    """Top procedures by row count for one hospital from BigQuery; description = canonical_description."""
    limit = max(1, min(int(limit), 200))
    from google.cloud.bigquery import ScalarQueryParameter
    sql = f"""
    SELECT f.billing_code, f.billing_code_type,
           COALESCE(h.canonical_description, MAX(f.description)) AS description,
           COUNT(*) AS row_count
    FROM {_bq_table('fct_standard_charges_semantic')} f
    LEFT JOIN {_bq_table('dim_procedure_harmonized')} h
      ON f.billing_code = h.billing_code AND COALESCE(CAST(f.billing_code_type AS STRING), 'UNKNOWN') = h.billing_code_type
    WHERE f.hospital_id = @hospital_id
    GROUP BY f.billing_code, f.billing_code_type, h.canonical_description
    ORDER BY row_count DESC
    LIMIT {limit}
    """
    return _bq_query(sql, [ScalarQueryParameter("hospital_id", "STRING", hospital_id)])


@st.cache_data
def get_coverage_matrix(_mode: Optional[str] = None) -> pd.DataFrame:
    """
    Per-hospital coverage from agg_payer_plan_compare: distinct billing_code, payer_family, plan_family counts
    and total comparable rows. Joined to dim_hospital for hospital_name_clean. Cached.
    """
    sql = f"""
    SELECT
      a.hospital_id,
      COALESCE(ANY_VALUE(d.hospital_name_clean), a.hospital_id) AS hospital_name_clean,
      COUNT(DISTINCT a.billing_code) AS distinct_billing_codes,
      COUNT(DISTINCT a.payer_family) AS distinct_payer_family,
      COUNT(DISTINCT a.plan_family) AS distinct_plan_family,
      SUM(a.row_count) AS total_comparable_rows
    FROM {_bq_table('agg_payer_plan_compare')} a
    LEFT JOIN {_bq_table('dim_hospital')} d ON a.hospital_id = d.hospital_id
    GROUP BY a.hospital_id
    ORDER BY total_comparable_rows DESC
    """
    return _bq_query(sql)


@st.cache_data
def get_top_procedure_variants(limit: int = 50, _mode: Optional[str] = None) -> pd.DataFrame:
    """
    Top procedure codes by description_variants_count from dim_procedure_harmonized. Cached.
    """
    limit = max(1, min(int(limit), 500))
    sql = f"""
    SELECT billing_code, billing_code_type, canonical_description, description_variants_count
    FROM {_bq_table('dim_procedure_harmonized')}
    WHERE description_variants_count > 0
    ORDER BY description_variants_count DESC
    LIMIT {limit}
    """
    return _bq_query(sql)


@st.cache_data
def get_payer_family_variant_counts(limit: int = 50, _mode: Optional[str] = None) -> pd.DataFrame:
    """
    Payer_family with count of distinct payer_name_norm mapping to it (from dim_payer_harmonized).
    High count = mapping opportunity (many raw names map to one family). Cached.
    """
    limit = max(1, min(int(limit), 200))
    sql = f"""
    SELECT
      payer_family,
      COUNT(DISTINCT payer_name_norm) AS payer_name_norm_count
    FROM {_bq_table('dim_payer_harmonized')}
    GROUP BY payer_family
    HAVING COUNT(DISTINCT payer_name_norm) > 1
    ORDER BY payer_name_norm_count DESC
    LIMIT {limit}
    """
    return _bq_query(sql)


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
    """Top rate_amount rows (outliers) from BigQuery. Capped query. Uses hospital_name_clean and canonical_description for display."""
    limit = max(1, min(int(limit), 500))
    sql = f"""
    SELECT f.hospital_id, COALESCE(d.hospital_name_clean, f.hospital_name) AS hospital_name,
           f.billing_code,
           COALESCE(h.canonical_description, f.description) AS description,
           f.rate_category, f.rate_amount, f.payer_name, f.plan_name
    FROM {_bq_table('fct_standard_charges_semantic')} f
    LEFT JOIN {_bq_table('dim_hospital')} d ON f.hospital_id = d.hospital_id
    LEFT JOIN {_bq_table('dim_procedure_harmonized')} h
      ON f.billing_code = h.billing_code AND COALESCE(CAST(f.billing_code_type AS STRING), 'UNKNOWN') = h.billing_code_type
    ORDER BY f.rate_amount DESC
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


def get_display_and_billing_diagnostic() -> pd.DataFrame:
    """
    Diagnostic: confirm hospital_name_clean is human-readable and billing_code 99213
    appears normalized in app-facing tables. Returns one row per check with columns:
    check_name, result_summary, sample_value, ok (bool).
    """
    rows = []
    try:
        # 1) dim_hospital: hospital_name_clean should not look like a hash (hex length 32)
        sql_dh = f"""
        SELECT hospital_id, hospital_name_clean
        FROM {_bq_table('dim_hospital')}
        LIMIT 5
        """
        df_dh = _bq_query(sql_dh)
        if df_dh.empty:
            rows.append({"check_name": "dim_hospital_sample", "result_summary": "no rows", "sample_value": None, "ok": False})
        else:
            sample = df_dh.iloc[0]
            clean = str(sample.get("hospital_name_clean") or "")
            looks_like_hash = len(clean) == 32 and all(c in "0123456789abcdef" for c in clean.lower())
            rows.append({
                "check_name": "dim_hospital_hospital_name_clean",
                "result_summary": "hash-like" if looks_like_hash else "human-readable",
                "sample_value": clean[:50] + ("..." if len(clean) > 50 else ""),
                "ok": not looks_like_hash,
            })

        # 2) agg: billing_code 99213 should exist and be exactly '99213'
        sql_agg = f"""
        SELECT billing_code, billing_code_type, COUNT(*) AS cnt
        FROM {_bq_table('agg_hospital_procedure_compare')}
        WHERE TRIM(CAST(billing_code AS STRING)) = '99213'
           OR TRIM(CAST(billing_code AS STRING)) LIKE '%99213%'
        GROUP BY 1, 2
        LIMIT 5
        """
        df_agg = _bq_query(sql_agg)
        if df_agg.empty:
            rows.append({"check_name": "billing_code_99213", "result_summary": "no 99213 rows", "sample_value": None, "ok": False})
        else:
            exact = df_agg[df_agg["billing_code"].astype(str).str.strip() == "99213"]
            ok = not exact.empty
            sample_val = df_agg["billing_code"].iloc[0] if not df_agg.empty else None
            rows.append({
                "check_name": "billing_code_99213_normalized",
                "result_summary": "99213 exact" if ok else "99213 has variant (e.g. leading digit)",
                "sample_value": str(sample_val),
                "ok": ok,
            })
    except Exception as e:
        rows.append({"check_name": "diagnostic_error", "result_summary": str(e), "sample_value": None, "ok": False})
    return pd.DataFrame(rows)


