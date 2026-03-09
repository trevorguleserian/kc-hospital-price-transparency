"""
Top Codes by Type: QA-style view of top billing codes by row count from app-facing marts.
Uses agg_hospital_procedure_compare (valid codes only). Optional filters: billing_code_type, hospitals.
"""
from __future__ import annotations

import io
from typing import Optional

import pandas as pd
import streamlit as st

from lib import data, ui
from lib import debug

st.set_page_config(page_title="Top Codes by Type", page_icon="📋", layout="wide")
debug.require_bq_secrets_or_stop()
ui.render_sidebar()

ok, msg = data.ensure_data_available()
if not ok:
    st.error(msg)
    st.stop()

st.title("Top Codes by Type")
st.caption("Top billing codes by row count from app-facing comparison data (validated codes only). Use filters to narrow by code type or hospitals.")

# Billing code types from inventory or default list
code_type_options = ["— All —", "CPT", "HCPCS", "NDC", "REVENUE", "ICD_10_PCS", "ICD_10_CM", "DRG", "MS_DRG", "APC", "EAPG", "HIPPS", "RC", "CDM", "UNKNOWN"]
hospitals_df = data.load_dim_hospital(data.get_mode())
hospital_options: list[tuple[str, str]] = []
if not hospitals_df.empty and "hospital_id" in hospitals_df.columns:
    display_col = "hospital_display_name" if "hospital_display_name" in hospitals_df.columns else ("hospital_name_clean" if "hospital_name_clean" in hospitals_df.columns else "hospital_id")
    if display_col in hospitals_df.columns:
        hospital_options = list(hospitals_df[["hospital_id", display_col]].drop_duplicates().itertuples(index=False, name=None))
    else:
        hospital_options = [(str(h), str(h)) for h in hospitals_df["hospital_id"].astype(str).drop_duplicates()]

with st.form("top_codes_filters"):
    code_type_sel = st.selectbox("Billing code type", code_type_options)
    billing_code_type: Optional[str] = None if code_type_sel == "— All —" else code_type_sel

    hospital_labels = [f"{h[1]} ({h[0]})" for h in hospital_options]
    default_hospitals = st.multiselect(
        "Hospitals (optional; leave empty for all)",
        options=range(len(hospital_options)),
        format_func=lambda i: hospital_labels[i],
        default=[],
    )
    hospital_ids: Optional[list[str]] = None
    if default_hospitals is not None and len(default_hospitals) > 0:
        hospital_ids = [hospital_options[i][0] for i in default_hospitals]

    limit = st.number_input("Max rows", min_value=10, max_value=500, value=100, step=10)
    submitted = st.form_submit_button("Load top codes")

if submitted:
    with st.spinner("Loading top codes…"):
        df = data.get_top_codes_by_type(
            billing_code_type=billing_code_type,
            hospital_ids=hospital_ids,
            limit=limit,
        )
    st.session_state["top_codes_df"] = df
    st.session_state["top_codes_run"] = True

top_codes_df: Optional[pd.DataFrame] = st.session_state.get("top_codes_df")
if top_codes_df is not None and not top_codes_df.empty and st.session_state.get("top_codes_run"):
    st.divider()
    display_cols = ["billing_code", "billing_code_type", "canonical_description", "row_count", "hospitals_covered"]
    existing = [c for c in display_cols if c in top_codes_df.columns]
    st.dataframe(top_codes_df[existing], use_container_width=True)
    buf = io.StringIO()
    top_codes_df.to_csv(buf, index=False)
    st.download_button("Download as CSV", data=buf.getvalue(), file_name="top_codes_by_type.csv", mime="text/csv")
else:
    if not st.session_state.get("top_codes_run"):
        st.caption("Set filters above and click **Load top codes**.")
