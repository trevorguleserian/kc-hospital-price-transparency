"""
Metabase Executive Dashboard: embed a Metabase dashboard in the portfolio app via iframe.
URL is read from Streamlit secrets (METABASE_EMBED_URL) or environment variable; no hardcoded URL in app logic.
"""
from __future__ import annotations

import os

import streamlit as st
import streamlit.components.v1 as components

from lib import ui

st.set_page_config(
    page_title="Metabase Executive Dashboard",
    page_icon="📊",
    layout="wide",
)
ui.render_sidebar()

# ---------------------------------------------------------------------------
# Resolve embed URL: secrets > env; if missing, show warning and stop
# ---------------------------------------------------------------------------
embed_url: str | None = None
try:
    if "METABASE_EMBED_URL" in st.secrets:
        embed_url = st.secrets["METABASE_EMBED_URL"]
except Exception:
    pass
if not embed_url:
    embed_url = os.getenv("METABASE_EMBED_URL")

if not embed_url or not str(embed_url).strip():
    st.warning(
        "No Metabase embed URL configured. Set **METABASE_EMBED_URL** in Streamlit secrets "
        "(.streamlit/secrets.toml) or as an environment variable to display the dashboard here."
    )
    st.info(
        "See README.md for how to add METABASE_EMBED_URL. You can copy from "
        ".streamlit/secrets.template.toml if present."
    )
    st.stop()

embed_url = str(embed_url).strip()

# ---------------------------------------------------------------------------
# Title and caption
# ---------------------------------------------------------------------------
st.title("Metabase Executive Dashboard")
st.caption(
    "This page is the BI/dashboard layer of the project: Metabase dashboards embedded inside the Streamlit analytics product."
)

# ---------------------------------------------------------------------------
# Summary metric cards above the iframe
# ---------------------------------------------------------------------------
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Dashboard Type", "Executive BI")
with col2:
    st.metric("Subject Area", "Hospital Pricing")
with col3:
    st.metric("Powered By", "Metabase")

# ---------------------------------------------------------------------------
# About this dashboard (expander)
# ---------------------------------------------------------------------------
with st.expander("About this dashboard", expanded=False):
    st.markdown("""
This dashboard shows:

- **Executive-relevant CPT and MS-DRG comparisons** — Key procedure and DRG metrics for stakeholder review.
- **Negotiated comparable rates across hospitals** — Like-to-like rates (negotiated, gross, cash, min/max) so comparisons are meaningful.
- **Harmonized payer and procedure groupings** — Payer family, plan family, and canonical procedure descriptions from the pipeline marts.
- **Metabase embedded inside Streamlit** — Part of the analytics product layer: one portfolio app with both interactive Streamlit pages and embedded Metabase dashboards.
    """)

# ---------------------------------------------------------------------------
# Embedded Metabase dashboard (height ~1450, scrolling enabled)
# ---------------------------------------------------------------------------
IFRAME_HEIGHT = 1450
components.iframe(embed_url, height=IFRAME_HEIGHT, scrolling=True)
