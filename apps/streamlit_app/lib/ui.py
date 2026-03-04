"""Shared UI components for Streamlit MVP."""
import os
import streamlit as st

from . import data
from . import bq_auth

# Default procedure query and max rows for demo (kept limited for BigQuery safety)
DEMO_PROCEDURE_QUERY = "99213"
DEMO_RESULTS_LIMIT = 500


def _bigquery_instructions():
    return """
**To enable BigQuery:**
- **Streamlit Cloud:** In app settings → Secrets, add:
  - `gcp.project_id` — your GCP project ID
  - `gcp.dataset` — dataset name (e.g. `pt_analytics_marts`)
  - `gcp.service_account_json` — full service account JSON (paste the key file contents)
- **Local:** Set `GOOGLE_APPLICATION_CREDENTIALS` to a service account JSON path, and `BQ_PROJECT`, `BQ_DATASET`.
"""


def render_sidebar():
    """Sidebar: data source (Local / BigQuery), active path, data availability, demo controls."""
    with st.sidebar:
        # Init data source from env or default local (demo mode, no creds)
        if "app_data_source" not in st.session_state:
            st.session_state["app_data_source"] = (os.environ.get("APP_MODE") or "local").strip().lower()

        st.subheader("Data source")
        choice = st.radio(
            "Source",
            ["Local (demo)", "BigQuery"],
            index=1 if data.get_mode() == "bigquery" else 0,
            help="Local uses dbt/exports. BigQuery uses Streamlit secrets (Cloud) or GOOGLE_APPLICATION_CREDENTIALS + BQ_PROJECT/BQ_DATASET (local).",
            key="data_source_radio",
        )
        new_mode = "bigquery" if choice == "BigQuery" else "local"
        if st.session_state.get("app_data_source") != new_mode:
            st.session_state["app_data_source"] = new_mode
            st.rerun()

        # If BigQuery selected but not configured, default to Local and show friendly message
        if new_mode == "bigquery":
            client, err = bq_auth.get_bq_client()
            project_id, dataset, creds_source = bq_auth.get_bq_config()
            if err or client is None or not (project_id and dataset):
                st.warning("BigQuery not configured. Using Local (demo).")
                with st.expander("How to configure BigQuery"):
                    st.markdown(_bigquery_instructions())
                st.session_state["app_data_source"] = "local"
                st.rerun()

        st.caption(data.get_active_source_label())
        ok, msg = data.ensure_data_available()
        if not ok:
            st.error(msg)
            if data.get_mode() == "local":
                with st.expander("How to generate local exports"):
                    st.markdown(data.get_local_exports_instructions())
        else:
            st.success("Data available")

        # BigQuery status and verification when in BigQuery mode
        if data.get_mode() == "bigquery":
            project_id, dataset, creds_source = bq_auth.get_bq_config()
            with st.expander("BigQuery status"):
                st.text(f"Project: {project_id}")
                st.text(f"Dataset: {dataset}")
                st.text(f"Credentials: {creds_source}")
                ok_verify, msg_verify, count = bq_auth.verify_bigquery_marts()
                if ok_verify and count is not None:
                    st.success(f"Marts OK — fct_standard_charges_semantic: {count:,} rows")
                elif not ok_verify:
                    st.error(f"Marts check: {msg_verify}")

        st.divider()
        st.subheader("Demo")
        if st.button("Load Demo Selection", help="Pre-fill first hospital and a common procedure (e.g. 99213), then run search automatically."):
            st.session_state["demo_load"] = True
            st.rerun()
        if st.session_state.get("demo_load"):
            st.caption("Go to Search & Compare to run.")


def render_demo_story_card():
    """Compact card explaining rate categories (negotiated, gross, min/max) for demo context."""
    with st.container():
        st.markdown("**What you are seeing** — Each row is a charge record. **rate_category** indicates the type of price: **negotiated** = payer-specific contracted rate; **gross** = list (cash) price; **cash** = discounted cash price; **min** / **max** = minimum or maximum of a rate range. Compare the same procedure across payers or categories to see variation.")
