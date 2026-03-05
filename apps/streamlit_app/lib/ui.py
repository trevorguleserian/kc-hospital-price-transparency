"""Shared UI components for Streamlit MVP."""
import os
from typing import Optional

import streamlit as st

from . import data
from . import bq_auth

# Default procedure query and max rows for demo (kept limited for BigQuery safety)
DEMO_PROCEDURE_QUERY = "99213"
DEMO_RESULTS_LIMIT = 500


@st.cache_data(ttl=120)
def _cached_fct_semantic_count(project_id: str, dataset: str) -> tuple[bool, str, Optional[int]]:
    """Run SELECT COUNT(1) on fct_standard_charges_semantic; cached so sidebar is not slow."""
    from . import bq_auth
    client, err = bq_auth.get_bq_client()
    if err or not client:
        return False, err or "No BigQuery client", None
    try:
        job = client.query(f"SELECT COUNT(1) AS n FROM `{project_id}.{dataset}.fct_standard_charges_semantic`")
        row = next(job.result(), None)
        return True, "OK", int(row["n"]) if row else 0
    except Exception as e:
        return False, str(e).strip() or "Query failed", None


def render_sidebar():
    """Sidebar: BigQuery only (Cloud). Active source label, data availability, demo controls."""
    with st.sidebar:
        st.subheader("Data source")
        st.caption("BigQuery only (Cloud)")
        st.caption(data.get_active_source_label())
        ok, msg = data.ensure_data_available()
        if not ok:
            st.error(msg)
        else:
            st.success("Data available")

        # BigQuery diagnostic panel (resolved config + cached COUNT(1) on fct_standard_charges_semantic)
        if ok:
            project_id, dataset, location, creds_source = bq_auth.get_bq_config()
            with st.expander("BigQuery status"):
                st.text(f"Project: {project_id}")
                st.text(f"Dataset (marts): {dataset}")
                st.text(f"Location: {location}")
                st.text(f"Credentials: {creds_source}")
                ok_smoke, msg_smoke, dim_count = bq_auth.smoke_query_dim_hospital()
                if ok_smoke and dim_count is not None:
                    st.success(f"dim_hospital: {dim_count:,} rows")
                elif not ok_smoke:
                    st.error(f"Smoke (dim_hospital): {msg_smoke}")
                # Cached COUNT(1) so sidebar is not slow
                ok_marts, msg_marts, marts_count = _cached_fct_semantic_count(project_id, dataset)
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
