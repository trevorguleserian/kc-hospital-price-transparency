"""
Hospital Comparison: compare rates by hospital for a given procedure, payer/plan, rate category and unit.
Uses agg_hospital_procedure_compare; all queries parameterized. Caching by filter params.
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

st.set_page_config(page_title="Hospital Comparison", page_icon="🏥", layout="wide")
debug.require_bq_secrets_or_stop()
ui.render_sidebar()

ok, msg = data.ensure_data_available()
if not ok:
    st.error(msg)
    st.stop()

st.title("Hospital Comparison")
st.caption("Compare min, max, and approximate median rates by hospital for a selected procedure, payer/plan, and rate type. Like-to-like only (comparable rows; rate_category = 'other' excluded).")

with st.expander("Data contract (comparability)"):
    st.markdown("""
- **comparability_key** = `billing_code_type | rate_category | rate_unit` (like-to-like grouping).
- **is_comparable** = TRUE only for categories used in comparisons: negotiated, gross, cash, min, max, percentage. Unit required except for unitless categories (e.g. percentage).
- **other** is retained in the source for diagnostics but **excluded from comparison tables**; this page only shows comparable rows.
    """)

# ---------------------------------------------------------------------------
# Filter options (payer/plan from dim_payer_harmonized; hospitals from dim_hospital)
# ---------------------------------------------------------------------------
payer_harmonized = data.load_dim_payer_harmonized(data.get_mode())
hospitals_df = data.load_dim_hospital(data.get_mode())

payer_families = ["— Select —"]
if not payer_harmonized.empty and "payer_family" in payer_harmonized.columns:
    payer_families += payer_harmonized["payer_family"].astype(str).drop_duplicates().sort_values().tolist()

plan_families = ["— Select —"]
hospital_options: list[tuple[str, str]] = []  # (hospital_id, display_label)
if not hospitals_df.empty:
    if "hospital_id" in hospitals_df.columns:
        display_col = "hospital_name_clean" if "hospital_name_clean" in hospitals_df.columns else "hospital_name"
        if display_col in hospitals_df.columns:
            opts = hospitals_df[["hospital_id", display_col]].drop_duplicates()
            hospital_options = list(opts.itertuples(index=False, name=None))
        else:
            hospital_options = [(h, h) for h in hospitals_df.iloc[:, 0].astype(str).drop_duplicates()]
    else:
        hospital_options = [(str(h), str(h)) for h in hospitals_df.iloc[:, 0].astype(str).drop_duplicates()]

# ---------------------------------------------------------------------------
# Filters form
# ---------------------------------------------------------------------------
with st.form("comparison_filters"):
    billing_code = st.text_input("Billing code (required)", placeholder="e.g. 99213", value="").strip()
    billing_code_type_options = ["— Any —", "CPT", "HCPCS", "NDC", "REVENUE", "ICD-10-PCS", "UNKNOWN"]
    billing_code_type_sel = st.selectbox("Billing code type (optional)", billing_code_type_options)
    billing_code_type: Optional[str] = None if billing_code_type_sel == "— Any —" else billing_code_type_sel

    payer_family_sel = st.selectbox("Payer family (required)", payer_families)
    payer_family: Optional[str] = None if payer_family_sel == "— Select —" else payer_family_sel

    # Plan family options depend on payer_family (optional filter)
    if payer_family and not payer_harmonized.empty and "plan_family" in payer_harmonized.columns:
        plans = payer_harmonized.loc[payer_harmonized["payer_family"].astype(str) == str(payer_family), "plan_family"]
        plan_list = ["— Any —"] + plans.astype(str).drop_duplicates().sort_values().tolist()
    else:
        plan_list = ["— Any —"]
    plan_family_sel = st.selectbox("Plan family (optional)", plan_list)
    plan_family = None if plan_family_sel == "— Any —" else plan_family_sel

    rate_category_options = ["negotiated", "gross", "cash", "min", "max", "percentage"]
    rate_category = st.selectbox("Rate category (comparable only; 'other' excluded)", rate_category_options)
    rate_unit_options = ["dollars", "percent"]
    rate_unit = st.selectbox("Rate unit", rate_unit_options)

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

    submitted = st.form_submit_button("Run comparison")

# ---------------------------------------------------------------------------
# Run query and show results
# ---------------------------------------------------------------------------
if submitted:
    if not billing_code:
        st.error("Please enter a billing code.")
        st.stop()
    if not payer_family or payer_family_sel == "— Select —":
        st.error("Please select a payer family.")
        st.stop()

    with st.spinner("Loading comparison…"):
        df = data.get_hospital_comparison(
            billing_code=billing_code,
            payer_family=payer_family,
            rate_category=rate_category,
            rate_unit=rate_unit,
            plan_family=plan_family,
            billing_code_type=billing_code_type,
            hospital_ids=hospital_ids,
        )

    if df.empty:
        st.info("No comparable rows found for this combination. Try different filters (e.g. payer/plan, rate category) or ensure the agg table is built.")
        try:
            rejects = data.get_rejects_summary(billing_code=billing_code, rate_category=rate_category, rate_unit=rate_unit)
            if not rejects.empty:
                with st.expander("Why no results? Top reasons from rejected rows"):
                    st.dataframe(rejects, use_container_width=True)
                    st.caption("Rejected rows (is_comparable = FALSE) for this procedure/rate; sample columns show example payer/hospital.")
        except Exception:
            pass
        st.stop()

    # Ensure numeric for chart
    for col in ("min_rate", "max_rate", "approx_median_rate", "row_count"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    st.session_state["comparison_df"] = df
    st.session_state["comparison_filters_run"] = True

# Display from session state so it persists after form submit
comparison_df: Optional[pd.DataFrame] = st.session_state.get("comparison_df")
if comparison_df is not None and not comparison_df.empty and st.session_state.get("comparison_filters_run"):
    st.divider()
    if "canonical_description" in comparison_df.columns and comparison_df["canonical_description"].notna().any():
        desc = comparison_df["canonical_description"].iloc[0]
        if desc and str(desc).strip():
            st.caption(f"**Procedure:** {str(desc)[:200]}{'…' if len(str(desc)) > 200 else ''}")
    st.subheader("Comparison by hospital")

    # 1) Table
    display_cols = ["hospital_name_clean", "min_rate", "max_rate", "approx_median_rate", "row_count", "rate_category", "rate_unit"]
    existing = [c for c in display_cols if c in comparison_df.columns]
    st.dataframe(comparison_df[existing], use_container_width=True)

    # Download CSV
    buf_csv = io.StringIO()
    comparison_df.to_csv(buf_csv, index=False)
    st.download_button(
        "Download table as CSV",
        data=buf_csv.getvalue(),
        file_name="hospital_comparison.csv",
        mime="text/csv",
    )

    # 2) Bar chart (approx_median_rate by hospital_name_clean; optional min/max whiskers)
    st.subheader("Approximate median rate by hospital")
    plot_df = comparison_df.sort_values("approx_median_rate", ascending=True).dropna(subset=["approx_median_rate"])
    if plot_df.empty:
        st.caption("No numeric median rates to plot.")
    else:
        fig, ax = plt.subplots(figsize=(10, max(4, len(plot_df) * 0.25)))
        x = range(len(plot_df))
        labels = plot_df["hospital_name_clean"].astype(str).str[:40].tolist()
        medians = plot_df["approx_median_rate"].values
        ax.barh(x, medians, color="steelblue", alpha=0.85, label="Approx. median")
        if "min_rate" in plot_df.columns and "max_rate" in plot_df.columns:
            mins = plot_df["min_rate"].values
            maxs = plot_df["max_rate"].values
            ax.hlines(x, mins, maxs, colors="gray", linewidth=1.5, alpha=0.7, label="Min–Max")
        ax.set_yticks(x)
        ax.set_yticklabels(labels, fontsize=9)
        ax.set_xlabel("Rate")
        ax.legend(loc="lower right")
        ax.set_title("Approximate median rate by hospital (min–max whiskers when available)")
        fig.tight_layout()

        buf_png = io.BytesIO()
        fig.savefig(buf_png, format="png", dpi=150, bbox_inches="tight")
        buf_png.seek(0)
        png_bytes = buf_png.getvalue()
        st.pyplot(fig)
        plt.close(fig)
        st.download_button(
            "Download chart as PNG",
            data=png_bytes,
            file_name="hospital_comparison_chart.png",
            mime="image/png",
            key="download_chart_png",
        )

else:
    if not st.session_state.get("comparison_filters_run"):
        st.caption("Set filters above and click **Run comparison** to load data.")
