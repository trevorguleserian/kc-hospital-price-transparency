"""
Safe debug panel for Streamlit (no secret values).
Enabled only when st.secrets["DEBUG"] == "1" or env DEBUG == "1".
Shows: secret keys present, gcp_service_account presence, required field names only, BQ client/smoke status.
"""
from __future__ import annotations

import os
import sys
from collections.abc import Mapping
from typing import Tuple

import streamlit as st

# Service account dict keys we check for presence (names only; never log values).
GCP_SA_REQUIRED_FIELDS = ("type", "project_id", "private_key_id", "private_key", "client_email")

# Required top-level keys for BigQuery (Cloud). BQ_LOCATION can default to US in bq_auth but we report if missing.
BQ_REQUIRED_KEYS = ["BQ_PROJECT", "BQ_LOCATION", "BQ_DATASET_MARTS", "gcp_service_account"]


def _is_empty_value(v) -> bool:
    """True if value is considered missing/empty (no secret value inspected)."""
    if v is None:
        return True
    if isinstance(v, str):
        return not v.strip()
    return False


def _get_gcp_sa_raw():
    """Get gcp_service_account value from secrets; no values logged."""
    try:
        return st.secrets.get("gcp_service_account") if hasattr(st.secrets, "get") else getattr(st.secrets, "gcp_service_account", None)
    except Exception:
        return None


def get_gcp_sa_type() -> str:
    """Return type of gcp_service_account value for display (e.g. 'dict', 'AttrDict'). No secret values."""
    raw = _get_gcp_sa_raw()
    if raw is None:
        return "missing"
    if isinstance(raw, Mapping):
        return raw.__class__.__name__
    return type(raw).__name__


def get_gcp_sa_key_names() -> list[str]:
    """Return list of key names inside gcp_service_account if it is a mapping; else []. No values."""
    raw = _get_gcp_sa_raw()
    if not isinstance(raw, Mapping):
        return []
    return sorted(raw.keys())


def secrets_keys() -> list[str]:
    """Return sorted list of st.secrets top-level key names (no values)."""
    return sorted(_safe_secrets_keys())


def has_bq_secrets() -> tuple[bool, list[str]]:
    """
    Check presence of required BigQuery secrets (key names only).
    Returns (True, []) if all present and valid, else (False, missing_items).
    missing_items: top-level keys, or "gcp_service_account (not a mapping; got <type>)", or "gcp_service_account.<field>" for missing/empty subfields.
    """
    keys = _safe_secrets_keys()
    missing: list[str] = []

    for k in ("BQ_PROJECT", "BQ_LOCATION", "BQ_DATASET_MARTS"):
        if k not in keys and not os.environ.get(k):
            missing.append(k)

    has_gcp_sa_key = "gcp_service_account" in keys
    if not has_gcp_sa_key:
        missing.append("gcp_service_account")
    else:
        try:
            raw = _get_gcp_sa_raw()
            if raw is None:
                missing.append("gcp_service_account")
            elif not isinstance(raw, Mapping):
                missing.append(f"gcp_service_account (not a mapping; got {type(raw).__name__})")
            else:
                sa_keys = list(raw.keys())
                for f in GCP_SA_REQUIRED_FIELDS:
                    if f not in sa_keys:
                        missing.append(f"gcp_service_account.{f}")
                    elif _is_empty_value(raw[f]):
                        missing.append(f"gcp_service_account.{f}")
        except Exception:
            missing.append("gcp_service_account")

    return (len(missing) == 0, missing)


def safe_runtime_info() -> dict:
    """Return runtime info safe to display (no secrets): python version, platform, cwd, Streamlit Cloud flag, optional package versions."""
    info = {
        "python_version": sys.version.split()[0],
        "platform": sys.platform,
        "cwd": os.getcwd(),
        "streamlit_cloud": os.environ.get("STREAMLIT_SERVER_HEADLESS", "").strip().lower() in ("true", "1"),
    }
    try:
        import google.cloud.bigquery as _bq
        info["google_cloud_bigquery_version"] = getattr(_bq, "__version__", "unknown")
    except Exception:
        info["google_cloud_bigquery_version"] = None
    try:
        import pyarrow as _pa
        info["pyarrow_version"] = getattr(_pa, "__version__", "unknown")
    except Exception:
        info["pyarrow_version"] = None
    try:
        import db_dtypes
        info["db_dtypes_version"] = getattr(db_dtypes, "__version__", "unknown")
    except Exception:
        info["db_dtypes_version"] = None
    return info


def is_debug_enabled() -> bool:
    """True when st.secrets["DEBUG"] == "1" or env DEBUG == "1"."""
    try:
        if os.environ.get("DEBUG", "").strip() == "1":
            return True
        v = st.secrets.get("DEBUG") if hasattr(st.secrets, "get") else getattr(st.secrets, "DEBUG", None)
        return str(v).strip() == "1"
    except Exception:
        return False


def _safe_secrets_keys() -> list[str]:
    """Return list of top-level secret keys (no values)."""
    try:
        if hasattr(st.secrets, "keys"):
            return list(st.secrets.keys())
        return []
    except Exception:
        return []


def validate_bq_secrets() -> Tuple[bool, str]:
    """
    Check presence of required secrets for BigQuery (key names only).
    Returns (ok, message). Message never contains secret values.
    """
    keys = _safe_secrets_keys()
    has_gcp_sa = "gcp_service_account" in keys
    gcp_sa_field_names: list[str] = []
    if has_gcp_sa:
        try:
            raw = _get_gcp_sa_raw()
            if isinstance(raw, Mapping):
                gcp_sa_field_names = list(raw.keys())
            elif raw is not None:
                gcp_sa_field_names = ["(value is not a mapping)"]
        except Exception:
            gcp_sa_field_names = ["(error reading keys)"]

    missing_top = []
    if not has_gcp_sa:
        missing_top.append("gcp_service_account")
    for k in ("BQ_PROJECT", "BQ_DATASET_MARTS"):
        if k not in keys and not os.environ.get(k):
            missing_top.append(k)

    missing_sa_fields = [f for f in GCP_SA_REQUIRED_FIELDS if f not in gcp_sa_field_names] if has_gcp_sa else list(GCP_SA_REQUIRED_FIELDS)

    has_project = "BQ_PROJECT" in keys or bool(os.environ.get("BQ_PROJECT"))
    has_dataset = "BQ_DATASET_MARTS" in keys or bool(os.environ.get("BQ_DATASET_MARTS"))
    ok = has_gcp_sa and len(missing_sa_fields) == 0 and has_project and has_dataset
    msg_parts = []
    msg_parts.append(f"Secret keys present: {sorted(keys)}")
    msg_parts.append(f"gcp_service_account exists: {has_gcp_sa}")
    if has_gcp_sa:
        msg_parts.append(f"gcp_service_account field names (only): {sorted(gcp_sa_field_names)}")
        if missing_sa_fields:
            msg_parts.append(f"Missing required fields (names only): {sorted(missing_sa_fields)}")
    if missing_top:
        msg_parts.append(f"Missing top-level secrets or env: {missing_top}")
    return ok, "\n".join(msg_parts)


def bq_smoke_test() -> Tuple[bool, str]:
    """Run SELECT 1 in BigQuery. Returns (success, message). No secret values in message."""
    try:
        from . import bq_auth
        client, err = bq_auth.get_bq_client()
        if err or client is None:
            return False, f"Client creation failed: {err}"
        job = client.query("SELECT 1 AS n")
        row = next(job.result(), None)
        if row is None:
            return False, "SELECT 1 returned no row"
        return True, "SELECT 1 succeeded"
    except Exception as e:
        return False, f"Smoke test error: {type(e).__name__}: {str(e)[:200]}"


def bq_secrets_error_message() -> str:
    """
    Build full "BigQuery not configured" message (keys and types only, no secret values).
    Shows: present top-level keys, missing items (including subkeys), gcp_service_account value type.
    """
    ok, missing = has_bq_secrets()
    present = secrets_keys()
    sa_type = get_gcp_sa_type()
    lines = [
        "BigQuery selected but secrets are missing or incomplete.",
        "",
        "Present secrets keys: " + (", ".join(present) if present else "(none)"),
        "Missing items: " + ", ".join(missing),
        "",
        "gcp_service_account value type: " + sa_type,
    ]
    sa_keys = get_gcp_sa_key_names()
    if sa_keys:
        lines.append("gcp_service_account keys (names only): " + ", ".join(sa_keys))
    return "\n".join(lines)


def require_bq_secrets_or_stop() -> None:
    """If BigQuery required secrets are missing, show error (keys/types only, no values) and st.stop()."""
    ok, missing = has_bq_secrets()
    if ok:
        return
    st.error("BigQuery not configured")
    st.markdown("**Secrets are missing or incomplete.**")
    st.text("Present secrets keys: " + (", ".join(secrets_keys()) if secrets_keys() else "(none)"))
    st.text("Missing items: " + ", ".join(missing))
    st.text("gcp_service_account value type: " + get_gcp_sa_type())
    sa_key_names = get_gcp_sa_key_names()
    if sa_key_names:
        st.text("gcp_service_account keys (names only): " + ", ".join(sa_key_names))
    with st.expander("Expected TOML structure (keys only)"):
        st.code(
            "BQ_PROJECT = \"your-project\"\n"
            "BQ_LOCATION = \"US\"\n"
            "BQ_DATASET_MARTS = \"pt_analytics_marts\"\n\n"
            "[gcp_service_account]\n"
            "type = \"service_account\"\n"
            "project_id = \"...\"\n"
            "private_key_id = \"...\"\n"
            "private_key = \"...\"\n"
            "client_email = \"...\"",
            language="toml",
        )
    st.stop()


def render_debug_panel() -> None:
    """Render debug info in the sidebar (keys, presence, BQ status). No secret values."""
    if not is_debug_enabled():
        return
    with st.sidebar:
        with st.expander("Debug (no secrets)", expanded=True):
            st.caption("Enabled by DEBUG=1 in secrets or env. Never paste private keys in logs.")
            try:
                keys = _safe_secrets_keys()
                st.text(f"Secret keys: {sorted(keys)}")
                st.text(f"gcp_service_account present: {'gcp_service_account' in keys}")
                if "gcp_service_account" in keys:
                    try:
                        raw = _get_gcp_sa_raw()
                        if isinstance(raw, Mapping):
                            st.text(f"gcp_service_account fields (names only): {sorted(raw.keys())}")
                        else:
                            st.text("gcp_service_account: (not a mapping)")
                    except Exception as ex:
                        st.text(f"gcp_service_account: (error: {type(ex).__name__})")
                ok_validate, msg_validate = validate_bq_secrets()
                st.text(f"BQ secrets check: {'OK' if ok_validate else 'FAIL'}")
                st.text_area("Details", msg_validate, height=120, disabled=True)
                ok_smoke, msg_smoke = bq_smoke_test()
                st.text(f"BQ smoke (SELECT 1): {'OK' if ok_smoke else 'FAIL'}")
                if not ok_smoke:
                    st.caption(msg_smoke)
            except Exception as e:
                st.error(f"Debug panel error: {type(e).__name__}")
