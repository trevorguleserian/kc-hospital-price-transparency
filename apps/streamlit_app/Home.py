"""
MVP Home: overview dashboard — context, metrics, and procedure coverage by hospital (stacked by code type).
Run from repo root: streamlit run apps/streamlit_app/Home.py
BigQuery-only for Cloud runs.
"""
from __future__ import annotations

import io
from typing import Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
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

# ---------------------------------------------------------------------------
# Top section: context and executive summary
# ---------------------------------------------------------------------------
st.title("Hospital Price Transparency")
st.caption("Overview")

st.markdown("""
**What is hospital price transparency data?**  
Hospitals publish **standard charge** files: machine-readable data that include negotiated rates with payers,
gross charges, discounted cash prices, and min/max rates by procedure, payer, and plan. This dashboard
uses those files to support comparison and analysis.

**CMS Hospital Price Transparency regulation**  
The **Hospital Price Transparency** rule (CMS-1717-F2, effective Jan 2021) requires hospitals to make
standard charges public in a consumer-friendly format and as machine-readable files. This app uses the
machine-readable files so we can compare prices across hospitals and payers in a like-to-like way.

**Why these hospitals?**  
The hospitals in this dataset were selected for **project scope**: a focused set of facilities (e.g. regional
or pilot) to demonstrate comparison and data quality without scaling to all U.S. hospitals. The pipeline
can be extended to ingest more facilities as needed.

**Why this dashboard matters (executive summary)**  
- **Procedure coverage** — How many distinct procedures (by code type) does each hospital publish?  
- **Rate spread** — How do negotiated, cash, and gross rates compare across hospitals for the same procedure?  
- **Payer concentration** — Which payers and plans have the most coverage at each hospital?  
- **Outliers** — Where are the highest or lowest rates for a given procedure?  
- **Data quality** — How complete and comparable are the published files?
""")

# ---------------------------------------------------------------------------
# Metrics row
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# Stacked bar: distinct billing_code by hospital and billing_code_type (exclude APC)
# ---------------------------------------------------------------------------
st.subheader("Procedure coverage by hospital (distinct codes by type)")
EXCLUDE_TYPES = ["APC"]
breakdown = data.get_home_hospital_code_type_breakdown(exclude_types=EXCLUDE_TYPES)

if breakdown is not None and not breakdown.empty:
    # Sort hospitals by total_distinct_codes desc (query already does; ensure unique order for chart)
    hospital_order = (
        breakdown[["hospital_name_clean", "total_distinct_codes"]]
        .drop_duplicates()
        .sort_values("total_distinct_codes", ascending=False)
    )
    hospitals_ordered = hospital_order["hospital_name_clean"].tolist()
    code_types = breakdown["billing_code_type"].dropna().unique().tolist()

    # Pivot: index = hospital_name_clean (in desired order), columns = billing_code_type, values = distinct_codes
    pivot = breakdown.pivot_table(
        index="hospital_name_clean",
        columns="billing_code_type",
        values="distinct_codes",
        aggfunc="sum",
        fill_value=0,
    )
    # Reindex to hospital order; ensure all code types present
    for ct in code_types:
        if ct not in pivot.columns:
            pivot[ct] = 0
    pivot = pivot.reindex(hospitals_ordered).fillna(0)

    # Stacked bar chart (matplotlib): x = hospital_name_clean, y = distinct billing codes, stack = billing_code_type
    fig, ax = plt.subplots(figsize=(max(8, min(len(hospitals_ordered) * 0.45, 24)), 6))
    x = range(len(pivot.index))
    width = 0.8
    bottom = None
    colors = plt.cm.tab10.colors
    for i, col in enumerate(pivot.columns):
        vals = pivot[col].values
        ax.bar(x, vals, width, label=col, bottom=bottom, color=colors[i % len(colors)])
        if bottom is None:
            bottom = vals.copy()
        else:
            bottom = bottom + vals

    ax.set_xticks(x)
    # Truncate long hospital names for readability in Streamlit Cloud
    labels = [str(s)[:36] + ("…" if len(str(s)) > 36 else "") for s in pivot.index]
    ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=9)
    ax.set_ylabel("Distinct billing codes")
    ax.set_xlabel("Hospital (hospital_name_clean)")
    ax.legend(title="billing_code_type", bbox_to_anchor=(1.02, 1), loc="upper left", fontsize=8)
    ax.set_title("Distinct billing codes by hospital and code type (APC excluded)")
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    buf.seek(0)
    st.pyplot(fig)
    plt.close(fig)

    # CSV download
    csv = breakdown.to_csv(index=False)
    st.download_button(
        label="Download data as CSV",
        data=csv,
        file_name="home_hospital_code_type_breakdown.csv",
        mime="text/csv",
    )

    # PNG download (reuse buffer from fig.savefig)
    st.download_button(
        label="Download chart as PNG",
        data=buf.getvalue(),
        file_name="home_hospital_code_type_breakdown.png",
        mime="image/png",
    )
else:
    st.caption("No procedure coverage data (exclude APC). Run dbt and ensure semantic + dim_hospital are populated.")

# ---------------------------------------------------------------------------
# Suggested next views (executive-friendly)
# ---------------------------------------------------------------------------
st.subheader("Suggested next views")
st.markdown("""
- **Hospital Comparison** — Compare min, median, and max rates **by hospital** for a chosen procedure, payer/plan, and rate type (e.g. negotiated vs cash).
- **Payer / Plan Comparison** — Compare rates **by payer_family and plan_family** for a procedure and selected hospitals; see spread and concentration.
- **Hospital Profile** — Drill into one hospital: top procedures, payer coverage, and rate distribution.
- **Top outlier procedures** — See the highest (and lowest) published rates across the dataset to spot potential data issues or high-cost services.
- **Data Quality** — Null rates, UNKNOWN code types, and comparability reasons to assess file completeness and like-to-like coverage.
""")
