"""
MVP Home: overview dashboard (metrics, rate_category distribution, ingested_at range).
Run from repo root: streamlit run apps/streamlit_app/Home.py
"""
import streamlit as st

from lib import data, ui
from lib import debug

st.set_page_config(page_title="Price Transparency — Overview", page_icon="🏥", layout="wide")
ui.render_sidebar()
if debug.is_debug_enabled():
    debug.render_debug_panel()

st.title("Hospital Price Transparency")
st.caption("Overview")

ok, msg = data.ensure_data_available()
if not ok:
    st.error(msg)
    if data.get_mode() == "local":
        st.markdown(data.get_local_exports_instructions())
    st.stop()

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
