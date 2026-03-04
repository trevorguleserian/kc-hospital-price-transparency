"""
BigQuery credential and client handling for Streamlit.
Supports (1) Streamlit Cloud: st.secrets[gcp].service_account_json + project/dataset,
(2) Local dev: GOOGLE_APPLICATION_CREDENTIALS + BQ_PROJECT/BQ_DATASET (or ADC).
No credentials are committed; all secrets come from env or st.secrets.
"""
from __future__ import annotations

import json
import os
import tempfile
from typing import Optional, Tuple

import streamlit as st

# Credentials source for UI
CREDS_SOURCE_SECRETS = "secrets"
CREDS_SOURCE_ENV = "env"
CREDS_SOURCE_ADC = "adc"
CREDS_SOURCE_NONE = "none"

# Temp file path kept for app lifetime when using secrets (so Client can read it)
_secrets_keyfile_path: Optional[str] = None


def _get_gcp_secrets() -> Optional[dict]:
    """Return st.secrets['gcp'] if it exists and has content; else None."""
    try:
        gcp = st.secrets.get("gcp") if hasattr(st.secrets, "get") else getattr(st.secrets, "gcp", None)
        if gcp is None:
            return None
        return dict(gcp) if hasattr(gcp, "items") else None
    except Exception:
        return None


def get_bq_config() -> Tuple[str, str, str]:
    """
    Return (project_id, dataset, creds_source).
    project_id and dataset may be empty if not configured.
    creds_source: 'secrets' | 'env' | 'adc' | 'none'
    """
    gcp = _get_gcp_secrets()
    if gcp:
        project = (gcp.get("project_id") or os.environ.get("BQ_PROJECT") or os.environ.get("DBT_BQ_PROJECT") or "").strip()
        dataset = (gcp.get("dataset") or os.environ.get("BQ_DATASET") or os.environ.get("DBT_BQ_DATASET") or "pt_analytics_marts").strip()
        if not dataset:
            dataset = "pt_analytics_marts"
        if gcp.get("service_account_json"):
            return project, dataset, CREDS_SOURCE_SECRETS
        # secrets present but no key -> still use env/ADC for creds
        return project, dataset, CREDS_SOURCE_ENV if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") else CREDS_SOURCE_ADC

    project = (os.environ.get("BQ_PROJECT") or os.environ.get("DBT_BQ_PROJECT") or "").strip()
    dataset = (os.environ.get("BQ_DATASET") or os.environ.get("DBT_BQ_DATASET") or "pt_analytics_marts").strip()
    if not dataset:
        dataset = "pt_analytics_marts"
    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        return project, dataset, CREDS_SOURCE_ENV
    return project, dataset, CREDS_SOURCE_ADC


def get_bq_client() -> Tuple[Optional[object], Optional[str]]:
    """
    Return (client, None) on success or (None, error_message) on failure.
    Caller should use project from get_bq_config() for queries; client is created with project when available.
    """
    from google.cloud import bigquery

    project_id, dataset, creds_source = get_bq_config()

    # Streamlit Cloud: credentials must come from secrets (no local key file)
    gcp = _get_gcp_secrets()
    if gcp and gcp.get("service_account_json"):
        global _secrets_keyfile_path
        if _secrets_keyfile_path and os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") == _secrets_keyfile_path:
            # Already wrote temp file in a previous call
            try:
                if project_id:
                    client = bigquery.Client(project=project_id)
                else:
                    client = bigquery.Client()
                return client, None
            except Exception as e:
                return None, str(e).strip() or "BigQuery client error"
        try:
            sa = gcp["service_account_json"]
            if isinstance(sa, dict):
                sa_str = json.dumps(sa)
            else:
                sa_str = str(sa).strip()
            if not sa_str:
                return None, "BigQuery secrets: gcp.service_account_json is empty."
            obj = json.loads(sa_str)
            if not isinstance(obj, dict) or obj.get("type") != "service_account":
                return None, "BigQuery secrets: service_account_json must be a service account JSON object."

            fd = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8")
            fd.write(sa_str)
            fd.close()
            _secrets_keyfile_path = fd.name
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = _secrets_keyfile_path
            if project_id and not os.environ.get("GOOGLE_CLOUD_PROJECT"):
                os.environ["GOOGLE_CLOUD_PROJECT"] = project_id
        except json.JSONDecodeError as e:
            return None, f"BigQuery secrets: invalid JSON in service_account_json: {e}"
        except Exception as e:
            return None, f"BigQuery secrets setup failed: {e}"

    # Build client with explicit project when we have it (avoids "Project was not passed" on Cloud)
    try:
        if project_id:
            client = bigquery.Client(project=project_id)
        else:
            client = bigquery.Client()
        return client, None
    except Exception as e:
        err = str(e).strip()
        if not err:
            err = "Unknown BigQuery client error"
        if "GOOGLE_APPLICATION_CREDENTIALS" in err or "project" in err.lower():
            return None, (
                "BigQuery not configured: add Streamlit secrets (gcp.service_account_json, gcp.project_id, gcp.dataset) "
                "or set GOOGLE_APPLICATION_CREDENTIALS and BQ_PROJECT, BQ_DATASET for local."
            )
        return None, f"BigQuery: {err}"


def is_bigquery_configured() -> bool:
    """True if we can obtain a BQ client (or at least have project + creds path)."""
    client, err = get_bq_client()
    if err:
        return False
    return client is not None


def verify_bigquery_marts() -> Tuple[bool, str, Optional[int]]:
    """
    Run SELECT COUNT(*) FROM marts.fct_standard_charges_semantic LIMIT 1 (conceptually).
    Returns (success, message, row_count or None).
    Uses configured project/dataset from get_bq_config(); table is in the marts dataset.
    """
    from google.cloud import bigquery

    project_id, dataset, _ = get_bq_config()
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
