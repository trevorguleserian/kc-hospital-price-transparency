"""
BigQuery credential and client handling for Streamlit.
- Streamlit Cloud: use st.secrets["gcp_service_account"] (service account JSON) and st.secrets["bq"] (project, dataset, location).
- Local dev: fall back to ADC (GOOGLE_APPLICATION_CREDENTIALS + BQ_PROJECT/BQ_DATASET).
No credentials are committed.
"""
from __future__ import annotations

import json
import os
import tempfile
from typing import Optional, Tuple

import streamlit as st

# Defaults when not in secrets/env
DEFAULT_BQ_PROJECT = "pricing-transparency-portfolio"
DEFAULT_BQ_DATASET = "pt_analytics_marts"
DEFAULT_BQ_LOCATION = "US"

# Credentials source for UI
CREDS_SOURCE_SECRETS = "secrets"
CREDS_SOURCE_ENV = "env"
CREDS_SOURCE_ADC = "adc"
CREDS_SOURCE_NONE = "none"

# Exact keys required for Cloud (shown in validation error)
REQUIRED_SECRETS_KEYS = [
    'gcp_service_account  # full service account JSON (paste key file contents)',
    '[bq]  # or env BQ_PROJECT, BQ_DATASET',
    '  project = "pricing-transparency-portfolio"',
    '  dataset = "pt_analytics_marts"',
    '  location = "US"  # optional',
]

_secrets_keyfile_path: Optional[str] = None


def _get_secret(key: str, default: Optional[dict] = None):
    try:
        v = st.secrets.get(key) if hasattr(st.secrets, "get") else getattr(st.secrets, key, None)
        if v is not None and hasattr(v, "items"):
            return dict(v)
        return v
    except Exception:
        return default


def get_bq_config() -> Tuple[str, str, str, str]:
    """
    Return (project_id, dataset, location, creds_source).
    project_id/dataset/location come from st.secrets["bq"] with sensible defaults, then env.
    creds_source: 'secrets' | 'env' | 'adc' | 'none'
    """
    bq = _get_secret("bq") or {}
    project = (
        (bq.get("project") or os.environ.get("BQ_PROJECT") or os.environ.get("DBT_BQ_PROJECT") or DEFAULT_BQ_PROJECT)
    ).strip()
    dataset = (
        (bq.get("dataset") or os.environ.get("BQ_DATASET") or os.environ.get("DBT_BQ_DATASET") or DEFAULT_BQ_DATASET)
    ).strip()
    location = (
        (bq.get("location") or os.environ.get("BQ_LOCATION") or DEFAULT_BQ_LOCATION)
    ).strip()

    # Credentials: from secrets first, then env, then ADC
    sa = _get_service_account_from_secrets()
    if sa is not None:
        return project, dataset, location, CREDS_SOURCE_SECRETS
    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        return project, dataset, location, CREDS_SOURCE_ENV
    return project, dataset, location, CREDS_SOURCE_ADC


def _get_service_account_from_secrets() -> Optional[dict]:
    """Return service account dict from st.secrets['gcp_service_account'] or None."""
    try:
        raw = st.secrets.get("gcp_service_account") if hasattr(st.secrets, "get") else getattr(st.secrets, "gcp_service_account", None)
        if raw is None:
            return None
        if isinstance(raw, dict):
            return raw
        s = str(raw).strip()
        if not s:
            return None
        return json.loads(s)
    except Exception:
        return None


def get_bq_client() -> Tuple[Optional[object], Optional[str]]:
    """
    Return (client, None) on success or (None, error_message) on failure.
    Uses st.secrets["gcp_service_account"] when present; else ADC locally.
    """
    from google.cloud import bigquery

    project_id, dataset, location, creds_source = get_bq_config()
    sa = _get_service_account_from_secrets()

    if sa is not None:
        global _secrets_keyfile_path
        if _secrets_keyfile_path and os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") == _secrets_keyfile_path:
            try:
                client = bigquery.Client(project=project_id, location=location) if location else bigquery.Client(project=project_id)
                return client, None
            except Exception as e:
                return None, str(e).strip() or "BigQuery client error"
        if not isinstance(sa, dict) or sa.get("type") != "service_account":
            return None, "BigQuery secrets: gcp_service_account must be a service account JSON object."
        try:
            sa_str = json.dumps(sa)
            fd = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8")
            fd.write(sa_str)
            fd.close()
            _secrets_keyfile_path = fd.name
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = _secrets_keyfile_path
            if project_id and not os.environ.get("GOOGLE_CLOUD_PROJECT"):
                os.environ["GOOGLE_CLOUD_PROJECT"] = project_id
        except Exception as e:
            return None, f"BigQuery secrets setup failed: {e}"

    try:
        if location:
            client = bigquery.Client(project=project_id, location=location)
        else:
            client = bigquery.Client(project=project_id) if project_id else bigquery.Client()
        return client, None
    except Exception as e:
        err = str(e).strip() or "Unknown BigQuery client error"
        return None, (
            "BigQuery not configured. Required: add Streamlit secrets gcp_service_account and [bq] (project, dataset), "
            "or set GOOGLE_APPLICATION_CREDENTIALS and BQ_PROJECT/BQ_DATASET for local."
            if ("GOOGLE_APPLICATION_CREDENTIALS" in err or "project" in err.lower())
            else f"BigQuery: {err}"
        )


def validate_bigquery_secrets() -> Tuple[bool, str]:
    """
    Call when BigQuery mode is selected. Returns (ok, message).
    If not ok, message lists the exact required keys so the user can fix secrets.
    """
    bq = _get_secret("bq") or {}
    project = (bq.get("project") or os.environ.get("BQ_PROJECT") or DEFAULT_BQ_PROJECT).strip()
    dataset = (bq.get("dataset") or os.environ.get("BQ_DATASET") or DEFAULT_BQ_DATASET).strip()
    has_creds = _get_service_account_from_secrets() is not None or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    missing = []
    if not has_creds:
        missing.append("**gcp_service_account** — service account JSON (required on Streamlit Cloud; or set GOOGLE_APPLICATION_CREDENTIALS locally)")
    if not project:
        missing.append("**bq.project** (or env BQ_PROJECT)")
    if not dataset:
        missing.append("**bq.dataset** (or env BQ_DATASET)")

    if missing:
        return False, (
            "BigQuery is selected but secrets are missing or incomplete.\n\n"
            "**Required:**\n- " + "\n- ".join(missing) + "\n\n"
            "**Exact TOML** (Streamlit Cloud → Settings → Secrets): see README “Deploy to Streamlit Community Cloud” → BigQuery (Cloud)."
        )
    return True, ""


def is_bigquery_configured() -> bool:
    client, err = get_bq_client()
    return client is not None and err is None


def smoke_query_dim_hospital() -> Tuple[bool, str, Optional[int]]:
    """
    Smoke query: SELECT COUNT(*) FROM project.dataset.dim_hospital.
    Returns (success, message, row_count or None). Use in UI when BigQuery mode is selected.
    """
    from google.cloud import bigquery

    project_id, dataset, _location, _ = get_bq_config()
    if not project_id or not dataset:
        return False, "Project or dataset not set.", None
    client, err = get_bq_client()
    if err or client is None:
        return False, err or "No BigQuery client.", None
    table_id = f"`{project_id}.{dataset}.dim_hospital`"
    try:
        sql = f"SELECT COUNT(*) AS n FROM {table_id}"
        job = client.query(sql)
        row = next(job.result(), None)
        count = int(row["n"]) if row is not None else 0
        return True, "OK", count
    except Exception as e:
        return False, str(e).strip() or "Query failed", None


def verify_bigquery_marts() -> Tuple[bool, str, Optional[int]]:
    """
    SELECT COUNT(*) FROM project.dataset.fct_standard_charges_semantic.
    Returns (success, message, row_count or None).
    """
    from google.cloud import bigquery

    project_id, dataset, _ = get_bq_config()[:3]
    if not project_id or not dataset:
        return False, "Project or dataset not set.", None
    client, err = get_bq_client()
    if err or client is None:
        return False, err or "No BigQuery client.", None
    table_id = f"`{project_id}.{dataset}.fct_standard_charges_semantic`"
    try:
        sql = f"SELECT COUNT(*) AS n FROM {table_id} LIMIT 1"
        job = client.query(sql)
        row = next(job.result(), None)
        count = int(row["n"]) if row is not None else 0
        return True, "OK", count
    except Exception as e:
        return False, str(e).strip() or "Query failed", None
