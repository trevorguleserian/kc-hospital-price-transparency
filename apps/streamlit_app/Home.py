"""
MVP Home: overview dashboard (metrics, rate_category distribution, ingested_at range).
Run from repo root: streamlit run apps/streamlit_app/Home.py
BigQuery-only for Cloud runs.
"""
import streamlit as st

from lib import data, ui
from lib import debug

st.set_page_config(page_title="Price Transparency — Overview", page_icon="🏥", layout="wide")

# Guard: stop early if BigQuery secrets missing (no local path attempt)
debug.require_bq_secrets_or_stop()

ui.render_sidebar()

# Debug (safe): keys only, no secret values
with st.sidebar:
    with st.expander("Debug (safe)", expanded=False):
        st.text("Secret keys present: " + ", ".join(debug.secrets_keys()) if debug.secrets_keys() else "(none)")
        ok_bq, missing = debug.has_bq_secrets()
        if not ok_bq:
            st.text("Missing keys: " + ", ".join(missing))
        st.json(debug.safe_runtime_info())

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
