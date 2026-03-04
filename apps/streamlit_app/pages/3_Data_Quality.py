"""
Data Quality: null-rate checks, UNKNOWN billing_code_type counts, top outlier rates.
"""
import io
import json
import streamlit as st

from lib import data, ui

st.set_page_config(page_title="Data Quality", page_icon="📋", layout="wide")
ui.render_sidebar()

ok, msg = data.ensure_data_available()
if not ok:
    st.error(msg)
    if data.get_mode() == "local":
        st.markdown(data.get_local_exports_instructions())
    st.stop()

st.title("Data Quality")

dq = data.get_data_quality_metrics(data.get_mode())
st.subheader("Null rates (fact table)")
tot = dq.get("total_rows") or 0
st.caption(f"Total rows: {tot:,}")
col1, col2, col3, col4 = st.columns(4)
col1.metric("% null hospital_id", f"{dq.get('pct_null_hospital_id', 0)}%")
col2.metric("% null billing_code", f"{dq.get('pct_null_billing_code', 0)}%")
col3.metric("% null rate_amount", f"{dq.get('pct_null_rate_amount', 0)}%")
col4.metric("% null description", f"{dq.get('pct_null_description', 0)}%")

st.subheader("UNKNOWN billing_code_type")
st.metric("Count", dq.get("unknown_billing_code_type_count", 0))

st.subheader("Top outlier rates (highest rate_amount)")
outlier_limit = st.slider("Number of outliers to show", 10, 500, 100)
outliers = data.get_outlier_rates(limit=outlier_limit)
if outliers.empty:
    st.caption("No data.")
else:
    st.dataframe(outliers, use_container_width=True)
    buf = io.StringIO()
    outliers.to_csv(buf, index=False)
    st.download_button("Download outliers (CSV)", data=buf.getvalue().encode("utf-8"), mime="text/csv", file_name="dq_outliers.csv", key="dl_outliers_csv")

st.subheader("Export")
st.download_button("Download metrics (JSON)", data=json.dumps(dq, indent=2, default=str), mime="application/json", file_name="dq_metrics.json", key="dl_dq_json")
