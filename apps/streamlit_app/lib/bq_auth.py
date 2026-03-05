"""
BigQuery auth for Streamlit.
- Streamlit Cloud: credentials from st.secrets["gcp_service_account"] (dict);
  project/dataset/location from BQ_PROJECT, BQ_DATASET_MARTS, BQ_LOCATION in secrets or env.
- Uses google.oauth2.service_account.Credentials.from_service_account_info (no temp files).
- Local dev: fall back to ADC (GOOGLE_APPLICATION_CREDENTIALS + env BQ_*).
No credentials are committed.
"""
from __future__ import annotations

import json
import os
from typing import Optional, Tuple

import streamlit as st

DEFAULT_BQ_PROJECT = "pricing-transparency-portfolio"
DEFAULT_BQ_DATASET_MARTS = "pt_analytics_marts"
DEFAULT_BQ_LOCATION = "US"

CREDS_SOURCE_SECRETS = "secrets"
CREDS_SOURCE_ENV = "env"
CREDS_SOURCE_ADC = "adc"

REQUIRED_SECRETS_KEYS = [
    "gcp_service_account   # dict with type, project_id, private_key_id, private_key, client_email, ...",
    "BQ_PROJECT            # e.g. pricing-transparency-portfolio",
    "BQ_DATASET_MARTS      # e.g. pt_analytics_marts (default)",
    "BQ_LOCATION           # optional, default US",
]


def _secret(key: str) -> Optional[str]:
    try:
        v = st.secrets.get(key) if hasattr(st.secrets, "get") else getattr(st.secrets, key, None)
        if v is None:
            return None
        return str(v).strip() if v else None
    except Exception:
        return None


def get_bq_config() -> Tuple[str, str, str, str]:
    """
    Return (project_id, dataset_marts, location, creds_source).
    Reads BQ_PROJECT, BQ_DATASET_MARTS, BQ_LOCATION from secrets then env, then defaults.
    """
    project = (_secret("BQ_PROJECT") or os.environ.get("BQ_PROJECT") or DEFAULT_BQ_PROJECT).strip()
    dataset_marts = (_secret("BQ_DATASET_MARTS") or os.environ.get("BQ_DATASET_MARTS") or os.environ.get("BQ_DATASET") or DEFAULT_BQ_DATASET_MARTS).strip()
    location = (_secret("BQ_LOCATION") or os.environ.get("BQ_LOCATION") or DEFAULT_BQ_LOCATION).strip()

    sa = _get_service_account_from_secrets()
    if sa is not None:
        return project, dataset_marts, location, CREDS_SOURCE_SECRETS
    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        return project, dataset_marts, location, CREDS_SOURCE_ENV
    return project, dataset_marts, location, CREDS_SOURCE_ADC


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


def get_bq_missing_key_names() -> list[str]:
    """Return list of missing secret/key names only (no values). For error messages."""
    missing = []
    sa = _get_service_account_from_secrets()
    if sa is None and not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        missing.append("gcp_service_account")
    project = (_secret("BQ_PROJECT") or os.environ.get("BQ_PROJECT") or "").strip()
    if not project:
        missing.append("BQ_PROJECT")
    dataset = (_secret("BQ_DATASET_MARTS") or os.environ.get("BQ_DATASET_MARTS") or "").strip()
    if not dataset:
        missing.append("BQ_DATASET_MARTS")
    return missing


def get_bq_client() -> Tuple[Optional[object], Optional[str]]:
    """
    Return (client, None) on success or (None, error_message) on failure.
    Uses Credentials.from_service_account_info when gcp_service_account is in secrets; else ADC.
    Error message includes missing key names only (no secret values) and suggests mode.
    """
    from google.cloud import bigquery
    from google.oauth2 import service_account

    project_id, dataset_marts, location, _ = get_bq_config()
    sa = _get_service_account_from_secrets()

    def _err(msg: str) -> Tuple[Optional[object], Optional[str]]:
        missing = get_bq_missing_key_names()
        suffix = f" Missing keys (names only): {missing}." if missing else ""
        return None, f"{msg}{suffix} (Mode: bigquery)"

    if sa is not None:
        if not isinstance(sa, dict) or sa.get("type") != "service_account":
            return _err("BigQuery secrets: gcp_service_account must be a service account JSON object (dict).")
        try:
            creds = service_account.Credentials.from_service_account_info(sa)
            proj = project_id or (sa.get("project_id") or "")
            if not proj:
                return _err("BigQuery: set BQ_PROJECT in secrets or env, or use a service account JSON that includes project_id.")
            client = bigquery.Client(credentials=creds, project=proj, location=location or None)
            return client, None
        except Exception as e:
            return None, f"BigQuery credentials failed: {e}. Missing keys (names only): {get_bq_missing_key_names()} (Mode: bigquery)"

    try:
        client = bigquery.Client(project=project_id or None, location=location or None)
        return client, None
    except Exception as e:
        err = str(e).strip() or "Unknown BigQuery client error"
        missing = get_bq_missing_key_names()
        base = (
            "BigQuery not configured. Add Streamlit secrets: gcp_service_account, BQ_PROJECT, BQ_DATASET_MARTS (and optionally BQ_LOCATION). "
            "Or locally set GOOGLE_APPLICATION_CREDENTIALS and BQ_PROJECT, BQ_DATASET_MARTS."
            if ("project" in err.lower() or "GOOGLE_APPLICATION_CREDENTIALS" in err)
            else f"BigQuery: {err}"
        )
        return None, f"{base} Missing keys (names only): {missing}. (Mode: bigquery)"


def validate_bigquery_secrets() -> Tuple[bool, str]:
    """When BigQuery is selected, return (ok, message). If not ok, message lists missing subkeys (e.g. gcp_service_account.private_key)."""
    from . import debug
    ok, missing_items = debug.has_bq_secrets()
    if ok:
        return True, ""
    msg = debug.bq_secrets_error_message()
    return False, msg


def get_bq_config_summary() -> dict:
    """
    Safe config summary: project_id, dataset, location, creds_source, sa_type, has_private_key (bool).
    No secret values; has_private_key is True only if the key exists and is non-empty (value not inspected).
    """
    project_id, dataset_marts, location, creds_source = get_bq_config()
    sa = _get_service_account_from_secrets()
    sa_type: str = "missing"
    has_private_key_bool = False
    if sa is None:
        try:
            from . import debug
            sa_type = debug.get_gcp_sa_type()
        except Exception:
            pass
    else:
        sa_type = type(sa).__name__
        has_private_key_bool = "private_key" in sa and bool(sa.get("private_key"))
    return {
        "project_id": project_id or "",
        "dataset": dataset_marts or "",
        "location": location or "",
        "creds_source": creds_source,
        "sa_type": sa_type,
        "has_private_key": has_private_key_bool,
    }


def is_bigquery_configured() -> bool:
    client, err = get_bq_client()
    return client is not None and err is None


def smoke_query_dim_hospital() -> Tuple[bool, str, Optional[int]]:
    """SELECT COUNT(*) FROM project.dataset.dim_hospital. Returns (success, message, count or None)."""
    project_id, dataset, _location, _ = get_bq_config()
    if not project_id or not dataset:
        return False, "Project or dataset not set.", None
    client, err = get_bq_client()
    if err or client is None:
        return False, err or "No BigQuery client.", None
    table_id = f"`{project_id}.{dataset}.dim_hospital`"
    try:
        job = client.query(f"SELECT COUNT(*) AS n FROM {table_id}")
        row = next(job.result(), None)
        return True, "OK", int(row["n"]) if row else 0
    except Exception as e:
        return False, str(e).strip() or "Query failed", None


def get_fct_semantic_count_cached() -> Tuple[bool, str, Optional[int]]:
    """
    SELECT COUNT(1) FROM pt_analytics_marts.fct_standard_charges_semantic.
    Cached per (project, dataset) so sidebar diagnostic is not slow.
    """
    project_id, dataset, _location, _ = get_bq_config()
    if not project_id or not dataset:
        return False, "Project or dataset not set.", None
    client, err = get_bq_client()
    if err or client is None:
        return False, err or "No BigQuery client.", None
    table_id = f"`{project_id}.{dataset}.fct_standard_charges_semantic`"
    try:
        job = client.query(f"SELECT COUNT(1) AS n FROM {table_id}")
        row = next(job.result(), None)
        return True, "OK", int(row["n"]) if row else 0
    except Exception as e:
        return False, str(e).strip() or "Query failed", None


def verify_bigquery_marts() -> Tuple[bool, str, Optional[int]]:
    """SELECT COUNT(*) FROM project.dataset.fct_standard_charges_semantic. Returns (success, message, count or None)."""
    return get_fct_semantic_count_cached()
