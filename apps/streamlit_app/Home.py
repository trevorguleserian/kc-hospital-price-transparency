"""
Portfolio Home: recruiter-facing landing for the Hospital Price Transparency analytics product.
Run from repo root: streamlit run apps/streamlit_app/Home.py
No BigQuery required for this page to load.
"""
from __future__ import annotations

import os

import streamlit as st

st.set_page_config(
    page_title="Hospital Price Transparency",
    page_icon="🏥",
    layout="wide",
)

# Minimal sidebar for recruiter-facing app (Streamlit shows page nav automatically)

# ---------------------------------------------------------------------------
# Hero / project title
# ---------------------------------------------------------------------------
st.title("Hospital Price Transparency")
st.markdown("**Analytics pipeline and dashboard for CMS hospital price transparency data**")

# ---------------------------------------------------------------------------
# Summary: business problem and what the project does
# ---------------------------------------------------------------------------
st.header("What this project does")

st.markdown("""
Hospitals are required to publish **standard charges** in machine-readable form under the **Hospital Price Transparency** rule (CMS-1717-F2). This project:

- **Ingests** raw machine-readable files (CSV and JSON) from a selected set of hospitals.
- **Models** the data in **BigQuery** with **dbt**: staging, semantic facts, comparability logic, and harmonized dimensions (payer, procedure, hospital).
- **Delivers** an executive BI layer via **Looker Studio** (embedded in this app) and a clean portfolio experience in **Streamlit**.

The result is a single, recruiter-friendly product that demonstrates end-to-end analytics engineering: from raw files to curated marts to embedded dashboards.
""")

# ---------------------------------------------------------------------------
# Platform architecture
# ---------------------------------------------------------------------------
st.header("Platform architecture")

st.markdown("""
Data flows through the pipeline as follows:
""")
st.markdown("""
**Hospital MRF files** (machine-readable standard charge files)  
→ **Python ingestion / validation** (bronze landing)  
→ **Bronze / Silver processing** (raw and standardized layers)  
→ **dbt models in BigQuery** (staging, semantic fact, comparable fact, harmonized dims, comparison marts)  
→ **Looker Studio dashboards** (executive BI on BigQuery marts)  
→ **Streamlit embedded analytics app** (this product: Home + Executive BI Dashboard)
""")

# ---------------------------------------------------------------------------
# What this demonstrates (recruiter-facing)
# ---------------------------------------------------------------------------
st.header("What this demonstrates")

st.markdown("""
- **Analytics engineering** — End-to-end pipeline from raw data to production-ready marts and BI.
- **Healthcare pricing data modeling** — Semantic rate categories, comparability logic, and like-to-like comparison design.
- **Billing code harmonization and governance** — Canonical procedure and payer groupings; validation and rejects handling.
- **BigQuery + dbt semantic marts** — Star-style dimensions and facts; tests and guardrails for comparison integrity.
- **Embedded BI delivery** — Looker Studio reports embedded in Streamlit as a unified analytics product.
""")

# ---------------------------------------------------------------------------
# Tech stack
# ---------------------------------------------------------------------------
st.header("Tech stack")

col1, col2, col3 = st.columns(3)
with col1:
    st.markdown("""
| Layer | Technology |
|-------|------------|
| Warehouse | Google BigQuery |
| Transform | dbt Core |
| BI | Looker Studio |
""")
with col2:
    st.markdown("""
| Layer | Technology |
|-------|------------|
| App | Streamlit |
| Language | Python 3.10+ |
| Auth (Cloud) | GCP service account |
""")
with col3:
    st.markdown("""
| Layer | Technology |
|-------|------------|
| Data I/O | pandas, pyarrow |
| Version control | Git / GitHub |
""")

# ---------------------------------------------------------------------------
# Call to action: Executive BI Dashboard
# ---------------------------------------------------------------------------
st.header("Explore the dashboard")

st.markdown("""
The **Executive BI Dashboard** page embeds a Looker Studio report built on the same BigQuery/dbt marts. It shows executive-relevant CPT and MS-DRG comparisons, hospital-to-hospital pricing variation, and harmonized payer and procedure groupings.
""")

if st.button("Open Executive BI Dashboard", type="primary"):
    st.switch_page("pages/2_Executive_BI_Dashboard.py")

st.caption("You can also use the **Executive BI Dashboard** link in the sidebar.")

# ---------------------------------------------------------------------------
# Optional: GitHub / repo link
# ---------------------------------------------------------------------------
repo_url = None
try:
    if "GITHUB_REPO_URL" in st.secrets:
        repo_url = st.secrets["GITHUB_REPO_URL"]
except Exception:
    pass
if not repo_url:
    repo_url = os.getenv("GITHUB_REPO_URL")
if repo_url and str(repo_url).strip():
    st.divider()
    st.markdown(f"**Repository:** [{str(repo_url).strip()}]({str(repo_url).strip()})")
