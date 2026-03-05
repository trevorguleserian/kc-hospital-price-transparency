"""
Safe debug panel for Streamlit (no secret values).
Enabled only when st.secrets["DEBUG"] == "1" or env DEBUG == "1".
Shows: secret keys present, gcp_service_account presence, required field names only, BQ client/smoke status.
"""
from __future__ import annotations

import os
from typing import Tuple

import streamlit as st

# Service account dict keys we check for presence (names only; never log values).
GCP_SA_REQUIRED_FIELDS = ("type", "project_id", "private_key_id", "private_key", "client_email")


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
            raw = st.secrets.get("gcp_service_account") if hasattr(st.secrets, "get") else getattr(st.secrets, "gcp_service_account", None)
            if isinstance(raw, dict):
                gcp_sa_field_names = list(raw.keys())
            elif raw is not None:
                gcp_sa_field_names = ["(value is not a dict)"]
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
                        raw = st.secrets.get("gcp_service_account") if hasattr(st.secrets, "get") else getattr(st.secrets, "gcp_service_account", None)
                        if isinstance(raw, dict):
                            st.text(f"gcp_service_account fields (names only): {sorted(raw.keys())}")
                        else:
                            st.text("gcp_service_account: (not a dict)")
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
