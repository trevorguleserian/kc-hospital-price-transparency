"""
MVP Home: overview dashboard (metrics, rate_category distribution, ingested_at range).
Run from repo root: streamlit run apps/streamlit_app/Home.py
BigQuery-only for Cloud runs.
"""
import streamlit as st

from lib import data, ui
from lib import debug
from lib import bq_auth

st.set_page_config(page_title="Price Transparency — Overview", page_icon="🏥", layout="wide")

# Guard: stop early if BigQuery secrets missing (no local path attempt)
debug.require_bq_secrets_or_stop()

ui.render_sidebar()

# Debug (safe): keys and types only, no secret values
with st.sidebar:
    with st.expander("Debug (safe)", expanded=False):
        info = debug.safe_runtime_info()
        bq_ver = info.get("google_cloud_bigquery_version")
        pa_ver = info.get("pyarrow_version")
        db_ver = info.get("db_dtypes_version")
        runtime_line = f"Python {info.get('python_version', '')} | {info.get('platform', '')}"
        if bq_ver:
            runtime_line += f" | bigquery {bq_ver}"
        if pa_ver:
            runtime_line += f" | pyarrow {pa_ver}"
        else:
            runtime_line += " | pyarrow not installed"
        if db_ver:
            runtime_line += f" | db_dtypes {db_ver}"
        else:
            runtime_line += " | db_dtypes not installed"
        st.caption(runtime_line)
        st.text("Secret keys present: " + ", ".join(debug.secrets_keys()) if debug.secrets_keys() else "(none)")
        st.text("gcp_service_account type: " + debug.get_gcp_sa_type())
        sa_keys = debug.get_gcp_sa_key_names()
        if sa_keys:
            st.text("gcp_service_account keys (names only): " + ", ".join(sa_keys))
        ok_bq, missing = debug.has_bq_secrets()
        if not ok_bq:
            st.text("Missing items: " + ", ".join(missing))
        st.json(info)
        try:
            st.json(bq_auth.get_bq_config_summary())
        except Exception:
            pass

ok, msg = data.ensure_data_available()
if not ok:
    st.error(msg)
    st.stop()

if debug.is_debug_enabled():
    debug.render_debug_panel()

st.title("Hospital Price Transparency")
st.caption("Overview")

metrics = data.get_overview_metrics(data.get_mode())
cols = st.columns(4)
cols[0].metric("Charge rows", f"{metrics['charges_rows']:,}")
cols[1].metric("Hospitals", metrics["hospitals_rows"])
cols[2].metric("Procedures", f"{metrics['procedures_rows']:,}")
cols[3].metric("Payers", metrics["payers_rows"])

st.subheader("Ingested date range")
min_at = metrics.get("min_ingested_at")
max_at = metrics.get("max_ingested_at")
if min_at or max_at:
    st.text(f"From {min_at} to {max_at}")
else:
    st.caption("No ingested_at range in data.")

st.subheader("Rate category distribution")
dist = data.get_rate_category_distribution(data.get_mode())
if dist is not None and not dist.empty:
    st.bar_chart(dist.set_index("rate_category")["cnt"])
else:
    st.caption("No rate category data.")
