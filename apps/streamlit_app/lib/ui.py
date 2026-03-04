"""Shared UI components for Streamlit MVP."""
import os
import streamlit as st

from . import data

# Default procedure query and max rows for demo (kept limited for BigQuery safety)
DEMO_PROCEDURE_QUERY = "99213"
DEMO_RESULTS_LIMIT = 500


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
            help="Local uses dbt/exports (CSV/Parquet). No credentials required. BigQuery requires GOOGLE_APPLICATION_CREDENTIALS.",
            key="data_source_radio",
        )
        new_mode = "bigquery" if choice == "BigQuery" else "local"
        if st.session_state.get("app_data_source") != new_mode:
            st.session_state["app_data_source"] = new_mode
            st.rerun()
        st.caption(data.get_active_source_label())
        ok, msg = data.ensure_data_available()
        if not ok:
            st.error(msg)
        else:
            st.success("Data available")

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
