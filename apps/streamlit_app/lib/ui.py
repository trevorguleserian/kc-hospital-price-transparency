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
**To enable BigQuery (Streamlit Cloud):** In app settings → Secrets, paste the exact TOML block from the README "Streamlit Cloud" section. Required keys: `gcp_service_account`, `[bq]` with `project` and `dataset`.

**Local:** Set `GOOGLE_APPLICATION_CREDENTIALS` to a service account JSON path, and `BQ_PROJECT`, `BQ_DATASET`.
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

        # If BigQuery selected, validate secrets on startup; show exact required keys if missing
        if new_mode == "bigquery":
            ok_val, validation_msg = bq_auth.validate_bigquery_secrets()
            if not ok_val:
                st.error("BigQuery not configured. Using Local (demo).")
                with st.expander("Required secrets (click to see exact keys)", expanded=True):
                    st.markdown(validation_msg)
                st.session_state["app_data_source"] = "local"
                st.rerun()
            else:
                client, err = bq_auth.get_bq_client()
                if err or client is None:
                    st.error(err or "BigQuery client failed. Using Local (demo).")
                    with st.expander("Required secrets (exact keys)", expanded=True):
                        st.code("\n".join(bq_auth.REQUIRED_SECRETS_KEYS), language="toml")
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

        # BigQuery status, smoke query (dim_hospital), and marts check when in BigQuery mode
        if data.get_mode() == "bigquery":
            project_id, dataset, location, creds_source = bq_auth.get_bq_config()
            with st.expander("BigQuery status"):
                st.text(f"Project: {project_id}")
                st.text(f"Dataset: {dataset}")
                st.text(f"Location: {location}")
                st.text(f"Credentials: {creds_source}")
                ok_smoke, msg_smoke, dim_count = bq_auth.smoke_query_dim_hospital()
                if ok_smoke and dim_count is not None:
                    st.success(f"dim_hospital: {dim_count:,} rows")
                elif not ok_smoke:
                    st.error(f"Smoke query (dim_hospital): {msg_smoke}")
                ok_marts, msg_marts, marts_count = bq_auth.verify_bigquery_marts()
                if ok_marts and marts_count is not None:
                    st.success(f"fct_standard_charges_semantic: {marts_count:,} rows")
                elif not ok_marts:
                    st.error(f"Marts check: {msg_marts}")

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
