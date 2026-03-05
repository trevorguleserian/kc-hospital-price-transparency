"""
Hospital Profile: KPIs, payer coverage, top procedures by count.
BigQuery-only for Cloud runs.
"""
import io
import json
import streamlit as st

from lib import data, ui
from lib import debug

st.set_page_config(page_title="Hospital Profile", page_icon="🏥", layout="wide")
debug.require_bq_secrets_or_stop()
ui.render_sidebar()

ok, msg = data.ensure_data_available()
if not ok:
    st.error(msg)
    st.stop()

st.title("Hospital Profile")

hospitals_df = data.load_dim_hospital(data.get_mode())
if hospitals_df.empty:
    st.warning("No hospitals in data.")
    st.stop()

if "hospital_id" in hospitals_df.columns and "hospital_name" in hospitals_df.columns:
    opts = hospitals_df[["hospital_id", "hospital_name"]].drop_duplicates()
    hospital_options = list(opts.itertuples(index=False, name=None))
    hospital_labels = [f"{h[1]} ({h[0]})" for h in hospital_options]
    hospital_ids = [h[0] for h in hospital_options]
else:
    hospital_labels = list(hospitals_df.iloc[:, 0].astype(str))
    hospital_ids = hospital_labels

sel_idx = st.selectbox("Hospital", range(len(hospital_labels)), format_func=lambda i: hospital_labels[i])
hospital_id = str(hospital_ids[sel_idx])

top_proc_limit = st.sidebar.slider("Top procedures to show", 10, 100, 50)
payer_limit = st.sidebar.slider("Payer coverage rows", 10, 200, 50)

st.subheader("KPIs")
kpis = data.get_hospital_kpis(hospital_id)
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total rate rows", f"{kpis['total_rows']:,}")
c2.metric("Distinct procedures", kpis["distinct_procedures"])
c3.metric("Payer/plan combos", kpis["distinct_payer_plan"])
c4.metric("Median rate", f"${kpis['median_rate']:,.0f}" if kpis.get("median_rate") is not None else "—")
c5.metric("Min / Max rate", f"${kpis['min_rate']:,.0f} / ${kpis['max_rate']:,.0f}" if kpis.get("min_rate") is not None else "—")

st.subheader("Payer coverage (row counts)")
payer_df = data.get_hospital_payer_coverage(hospital_id, limit=payer_limit)
if payer_df.empty:
    st.caption("No payer coverage data.")
else:
    st.dataframe(payer_df, use_container_width=True)
    buf = io.StringIO()
    payer_df.to_csv(buf, index=False)
    st.download_button("Download payer coverage (CSV)", data=buf.getvalue().encode("utf-8"), mime="text/csv", file_name="hospital_payer_coverage.csv", key="dl_payer_csv")

st.subheader("Top procedures by count")
top_df = data.get_hospital_top_procedures(hospital_id, limit=top_proc_limit)
if top_df.empty:
    st.caption("No procedure counts.")
else:
    st.dataframe(top_df, use_container_width=True)
    buf2 = io.StringIO()
    top_df.to_csv(buf2, index=False)
    st.download_button("Download top procedures (CSV)", data=buf2.getvalue().encode("utf-8"), mime="text/csv", file_name="hospital_top_procedures.csv", key="dl_top_csv")

st.subheader("Export summary")
summary = {"hospital_id": hospital_id, "kpis": kpis, "payer_coverage_rows": len(payer_df), "top_procedures_rows": len(top_df)}
st.download_button("Download summary (JSON)", data=json.dumps(summary, indent=2, default=str), mime="application/json", file_name="hospital_profile_summary.json", key="dl_summary_json")
