"""
Executive BI Dashboard: embed a Looker Studio report in the portfolio app via iframe.
URL is read from Streamlit secrets (LOOKER_STUDIO_EMBED_URL), environment variable, or fallback default.
"""
from __future__ import annotations

import os

import streamlit as st
import streamlit.components.v1 as components

# Fallback embed URL when secrets/env are not set (production-friendly default for portfolio).
DEFAULT_LOOKER_STUDIO_EMBED_URL = (
    "https://lookerstudio.google.com/embed/reporting/c2676e11-d089-4281-b4f5-ea81f03603d1/page/RhcrF"
)

st.set_page_config(
    page_title="Executive BI Dashboard",
    page_icon="📊",
    layout="wide",
)

# Minimal sidebar: no BigQuery/data-source clutter for recruiter-facing app
# (Streamlit still shows page nav in sidebar automatically.)

# ---------------------------------------------------------------------------
# Resolve embed URL: secrets > env > fallback default (always render if URL present)
# ---------------------------------------------------------------------------
embed_url: str | None = None
try:
    if "LOOKER_STUDIO_EMBED_URL" in st.secrets:
        embed_url = st.secrets["LOOKER_STUDIO_EMBED_URL"]
except Exception:
    pass
if not embed_url:
    embed_url = os.getenv("LOOKER_STUDIO_EMBED_URL")
if not embed_url or not str(embed_url).strip():
    embed_url = DEFAULT_LOOKER_STUDIO_EMBED_URL

embed_url = str(embed_url).strip()

# ---------------------------------------------------------------------------
# Title and caption
# ---------------------------------------------------------------------------
st.title("Healthcare Pricing Executive Dashboard")
st.caption(
    "BI/dashboard layer powered by Looker Studio, embedded in this Streamlit analytics product."
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
    st.metric("Powered By", "Looker Studio")

# ---------------------------------------------------------------------------
# About this dashboard (expander)
# ---------------------------------------------------------------------------
with st.expander("About this dashboard", expanded=False):
    st.markdown("""
This dashboard shows:

- **Executive-relevant CPT and MS-DRG comparisons** — Key procedure and DRG metrics for stakeholder review.
- **Hospital-to-hospital pricing variation** — How rates for the same procedure and payer vary across facilities.
- **Harmonized payer and procedure groupings** — Payer family, plan family, and canonical procedure descriptions from the pipeline marts.
- **Dashboards built on curated BigQuery/dbt marts** — The same semantic and comparison marts that power the analytics pipeline.
- **Embedded inside Streamlit** — Part of the analytics product layer: one portfolio app with both a narrative Home and embedded Looker Studio dashboards.
    """)

# ---------------------------------------------------------------------------
# Embedded Looker Studio dashboard (width fills page, height 1400–1600, scrolling)
# ---------------------------------------------------------------------------
IFRAME_HEIGHT = 1500
components.iframe(embed_url, height=IFRAME_HEIGHT, scrolling=True)
