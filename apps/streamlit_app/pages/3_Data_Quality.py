"""
Data Quality: null-rate checks, UNKNOWN billing_code_type, top outlier rates,
coverage matrix (per-hospital comparable stats), and variant flags (procedure + payer mapping).
BigQuery-only; sourced from dbt marts/harmonized dims where noted.
"""
import io
import json
import streamlit as st

from lib import data, ui
from lib import debug

st.set_page_config(page_title="Data Quality", page_icon="📋", layout="wide")
debug.require_bq_secrets_or_stop()
ui.render_sidebar()

ok, msg = data.ensure_data_available()
if not ok:
    st.error(msg)
    st.stop()

st.title("Data Quality")

# ---------------------------------------------------------------------------
# Existing: null rates and UNKNOWN billing_code_type
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# 1) Coverage matrix (per hospital: distinct billing_code, payer_family, plan_family, total comparable rows)
# ---------------------------------------------------------------------------
st.divider()
st.subheader("Coverage matrix (per hospital)")
st.caption("From agg_payer_plan_compare: distinct billing codes, payer_family, plan_family counts and total comparable rows per hospital.")
coverage = data.get_coverage_matrix(data.get_mode())
if coverage.empty:
    st.caption("No coverage data (agg_payer_plan_compare may not be built yet).")
else:
    # Show hospital_name_clean first for readability
    display_cols = ["hospital_name_clean", "hospital_id", "distinct_billing_codes", "distinct_payer_family", "distinct_plan_family", "total_comparable_rows"]
    display_cols = [c for c in display_cols if c in coverage.columns]
    st.dataframe(coverage[display_cols], use_container_width=True)
    buf_cov = io.StringIO()
    coverage.to_csv(buf_cov, index=False)
    st.download_button("Download coverage matrix (CSV)", data=buf_cov.getvalue(), mime="text/csv", file_name="dq_coverage_matrix.csv", key="dl_coverage_csv")

# ---------------------------------------------------------------------------
# 2) Variants flags: top procedure codes by description_variants_count; top payer_family by payer_name_norm count
# ---------------------------------------------------------------------------
st.divider()
st.subheader("Variant flags")

st.caption("**Procedure codes by description variant count** (dim_procedure_harmonized). High counts indicate many different descriptions for the same code.")
proc_var_limit = st.slider("Top N procedure variants", 10, 200, 50, key="proc_var_slider")
proc_variants = data.get_top_procedure_variants(limit=proc_var_limit, _mode=data.get_mode())
if proc_variants.empty:
    st.caption("No procedure variant data.")
else:
    st.dataframe(proc_variants, use_container_width=True)
    buf_proc = io.StringIO()
    proc_variants.to_csv(buf_proc, index=False)
    st.download_button("Download procedure variants (CSV)", data=buf_proc.getvalue(), mime="text/csv", file_name="dq_procedure_variants.csv", key="dl_proc_var_csv")

st.caption("**Payer families with multiple payer_name_norm** (dim_payer_harmonized). High counts reveal mapping opportunities (many raw names → one family).")
payer_var_limit = st.slider("Top N payer-family variant counts", 10, 200, 50, key="payer_var_slider")
payer_variants = data.get_payer_family_variant_counts(limit=payer_var_limit, _mode=data.get_mode())
if payer_variants.empty:
    st.caption("No payer family variant data.")
else:
    st.dataframe(payer_variants, use_container_width=True)
    buf_payer = io.StringIO()
    payer_variants.to_csv(buf_payer, index=False)
    st.download_button("Download payer family variants (CSV)", data=buf_payer.getvalue(), mime="text/csv", file_name="dq_payer_family_variants.csv", key="dl_payer_var_csv")

# ---------------------------------------------------------------------------
# Existing: top outlier rates and export
# ---------------------------------------------------------------------------
st.divider()
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
