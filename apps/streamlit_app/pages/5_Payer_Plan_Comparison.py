"""
Payer / Plan Comparison: compare payer_family and plan_family rates (e.g. negotiated) for a procedure and hospitals.
Uses agg_payer_plan_compare; all queries parameterized. Aggregates to payer and plan level in-app.
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

st.set_page_config(page_title="Payer / Plan Comparison", page_icon="📊", layout="wide")
debug.require_bq_secrets_or_stop()
ui.render_sidebar()

ok, msg = data.ensure_data_available()
if not ok:
    st.error(msg)
    st.stop()

st.title("Payer / Plan Comparison")
st.caption("Compare min, median, and max rates by payer_family and by plan_family for a procedure and selected hospitals. Like-to-like only (comparable rows; rate_category = 'other' excluded).")

with st.expander("Data contract (comparability)"):
    st.markdown("""
- **comparability_key** = `billing_code_type | rate_category | rate_unit` (like-to-like grouping).
- **is_comparable** = TRUE only for categories used in comparisons: negotiated, gross, cash, min, max, percentage. Unit required except for unitless categories (e.g. percentage).
- **other** is retained in the source for diagnostics but **excluded from comparison tables**; this page only shows comparable rows.
    """)

# ---------------------------------------------------------------------------
# Filter options (payer/plan from dim_payer_harmonized)
# ---------------------------------------------------------------------------
payer_harmonized = data.load_dim_payer_harmonized(data.get_mode())
payer_families = ["— Any —"]
if not payer_harmonized.empty and "payer_family" in payer_harmonized.columns:
    payer_families += payer_harmonized["payer_family"].astype(str).drop_duplicates().sort_values().tolist()

hospitals_df = data.load_dim_hospital(data.get_mode())
hospital_options: list[tuple[str, str]] = []
if not hospitals_df.empty:
    if "hospital_id" in hospitals_df.columns:
        display_col = "hospital_display_name" if "hospital_display_name" in hospitals_df.columns else ("hospital_name_clean" if "hospital_name_clean" in hospitals_df.columns else "hospital_name")
        if display_col in hospitals_df.columns:
            opts = hospitals_df[["hospital_id", display_col]].drop_duplicates()
            hospital_options = list(opts.itertuples(index=False, name=None))
        else:
            hospital_options = [(h, h) for h in hospitals_df.iloc[:, 0].astype(str).drop_duplicates()]
    else:
        hospital_options = [(str(h), str(h)) for h in hospitals_df.iloc[:, 0].astype(str).drop_duplicates()]

rate_category_options = ["negotiated", "gross", "cash", "min", "max", "percentage"]
rate_unit_options = ["dollars", "percent"]

# ---------------------------------------------------------------------------
# Filters form
# ---------------------------------------------------------------------------
with st.form("payer_plan_filters"):
    billing_code = st.text_input("Billing code (required)", placeholder="e.g. 99213", value="").strip()
    billing_code_type_options = ["— Any —", "CPT", "HCPCS", "NDC", "REVENUE", "ICD-10-PCS", "UNKNOWN"]
    billing_code_type_sel = st.selectbox("Billing code type (optional)", billing_code_type_options)
    billing_code_type = None if billing_code_type_sel == "— Any —" else billing_code_type_sel

    rate_category = st.selectbox("Rate category (required; comparable only, 'other' excluded)", rate_category_options, index=0)
    rate_unit = st.selectbox("Rate unit (required)", rate_unit_options)

    payer_family_sel = st.selectbox("Payer family (optional, recommended)", payer_families)
    payer_family = None if payer_family_sel == "— Any —" else payer_family_sel
    plan_list = ["— Any —"]
    if payer_family and not payer_harmonized.empty and "plan_family" in payer_harmonized.columns:
        plans = payer_harmonized.loc[payer_harmonized["payer_family"].astype(str) == str(payer_family), "plan_family"]
        plan_list += plans.astype(str).drop_duplicates().sort_values().tolist()
    plan_family_sel = st.selectbox("Plan family (optional)", plan_list)
    plan_family = None if plan_family_sel == "— Any —" else plan_family_sel

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
# Run query and build payer / plan aggregates
# ---------------------------------------------------------------------------
if submitted:
    if not billing_code:
        st.error("Please enter a billing code.")
        st.stop()

    with st.spinner("Loading comparison data…"):
        detail_df = data.get_payer_plan_compare_detail(
            billing_code=billing_code,
            rate_category=rate_category,
            rate_unit=rate_unit,
            payer_family=payer_family,
            plan_family=plan_family,
            billing_code_type=billing_code_type,
            hospital_ids=hospital_ids,
        )

    if detail_df.empty:
        st.info("No comparable rows found for this combination. Try different filters (e.g. payer/plan, hospitals) or ensure agg_payer_plan_compare is built.")
        try:
            rejects = data.get_rejects_summary(billing_code=billing_code, rate_category=rate_category, rate_unit=rate_unit)
            if not rejects.empty:
                with st.expander("Why no results? Top reasons from rejected rows"):
                    st.dataframe(rejects, use_container_width=True)
                    st.caption("Rejected rows (is_comparable = FALSE) for this procedure/rate; sample columns show example payer/hospital.")
        except Exception:
            pass
        st.stop()

    for col in ("min_rate", "max_rate", "approx_median_rate", "row_count"):
        if col in detail_df.columns:
            detail_df[col] = pd.to_numeric(detail_df[col], errors="coerce")

    # A) Payer-level: payer_family, min_rate, median (mean of approx_median), max_rate, row_count, hospitals_covered
    payer_agg = (
        detail_df.groupby("payer_family", as_index=False)
        .agg(
            min_rate=("min_rate", "min"),
            max_rate=("max_rate", "max"),
            approx_median_rate=("approx_median_rate", "mean"),
            row_count=("row_count", "sum"),
            hospitals_covered=("hospital_id", "nunique"),
        )
        .rename(columns={"approx_median_rate": "median_rate"})
    )
    payer_agg["rate_category"] = rate_category
    payer_agg["rate_unit"] = rate_unit

    st.session_state["payer_plan_detail_df"] = detail_df
    st.session_state["payer_plan_payer_agg"] = payer_agg
    st.session_state["payer_plan_filters_run"] = True
    st.session_state["payer_plan_rate_category"] = rate_category
    st.session_state["payer_plan_rate_unit"] = rate_unit

# ---------------------------------------------------------------------------
# Display from session state
# ---------------------------------------------------------------------------
filters_run = st.session_state.get("payer_plan_filters_run")
payer_agg = st.session_state.get("payer_plan_payer_agg")
detail_df = st.session_state.get("payer_plan_detail_df")

if filters_run and payer_agg is not None and not payer_agg.empty:
    st.divider()
    if detail_df is not None and not detail_df.empty and "canonical_description" in detail_df.columns and detail_df["canonical_description"].notna().any():
        desc = detail_df["canonical_description"].iloc[0]
        if desc and str(desc).strip():
            st.caption(f"**Procedure:** {str(desc)[:200]}{'…' if len(str(desc)) > 200 else ''}")

    # ---------- A) Payer comparison table ----------
    st.subheader("A) Payer comparison")
    payer_display = payer_agg[["payer_family", "min_rate", "median_rate", "max_rate", "row_count", "hospitals_covered", "rate_category", "rate_unit"]]
    st.dataframe(payer_display, use_container_width=True)
    buf_payer_csv = io.StringIO()
    payer_display.to_csv(buf_payer_csv, index=False)
    st.download_button("Download payer table as CSV", data=buf_payer_csv.getvalue(), file_name="payer_comparison.csv", mime="text/csv", key="dl_payer_csv")

    # Bar chart: median by payer_family
    st.subheader("Median rate by payer")
    plot_payer = payer_agg.sort_values("median_rate", ascending=True).dropna(subset=["median_rate"])
    if not plot_payer.empty:
        fig_payer, ax_payer = plt.subplots(figsize=(10, max(4, len(plot_payer) * 0.3)))
        x = range(len(plot_payer))
        ax_payer.barh(x, plot_payer["median_rate"].values, color="steelblue", alpha=0.85)
        ax_payer.set_yticks(x)
        ax_payer.set_yticklabels(plot_payer["payer_family"].astype(str).str[:50].tolist(), fontsize=9)
        ax_payer.set_xlabel("Median rate")
        ax_payer.set_title("Median rate by payer_family")
        fig_payer.tight_layout()
        buf_payer_png = io.BytesIO()
        fig_payer.savefig(buf_payer_png, format="png", dpi=150, bbox_inches="tight")
        buf_payer_png.seek(0)
        png_payer = buf_payer_png.getvalue()
        st.pyplot(fig_payer)
        plt.close(fig_payer)
        st.download_button("Download payer chart as PNG", data=png_payer, file_name="payer_comparison_chart.png", mime="image/png", key="dl_payer_png")

    # ---------- B) Plan comparison (for selected payer_family) ----------
    st.divider()
    st.subheader("B) Plan comparison (select a payer)")
    payer_families_in_data = payer_agg["payer_family"].astype(str).tolist()
    selected_payer = st.selectbox("Payer family (for plan view)", ["— Select —"] + payer_families_in_data, key="plan_payer_select")
    plan_agg = None
    if selected_payer and selected_payer != "— Select —" and detail_df is not None:
        plan_detail = detail_df[detail_df["payer_family"].astype(str) == str(selected_payer)]
        if not plan_detail.empty:
            plan_agg = (
                plan_detail.groupby("plan_family", as_index=False)
                .agg(
                    min_rate=("min_rate", "min"),
                    max_rate=("max_rate", "max"),
                    approx_median_rate=("approx_median_rate", "mean"),
                    row_count=("row_count", "sum"),
                )
                .rename(columns={"approx_median_rate": "median_rate"})
            )
            plan_agg["rate_category"] = st.session_state.get("payer_plan_rate_category", "")
            plan_agg["rate_unit"] = st.session_state.get("payer_plan_rate_unit", "")

    if plan_agg is not None and not plan_agg.empty:
        plan_display = plan_agg[["plan_family", "min_rate", "median_rate", "max_rate", "row_count", "rate_category", "rate_unit"]]
        st.dataframe(plan_display, use_container_width=True)
        buf_plan_csv = io.StringIO()
        plan_display.to_csv(buf_plan_csv, index=False)
        st.download_button("Download plan table as CSV", data=buf_plan_csv.getvalue(), file_name="plan_comparison.csv", mime="text/csv", key="dl_plan_csv")

        st.subheader("Median rate by plan")
        plot_plan = plan_agg.sort_values("median_rate", ascending=True).dropna(subset=["median_rate"])
        if not plot_plan.empty:
            fig_plan, ax_plan = plt.subplots(figsize=(10, max(4, len(plot_plan) * 0.25)))
            x_plan = range(len(plot_plan))
            ax_plan.barh(x_plan, plot_plan["median_rate"].values, color="seagreen", alpha=0.85)
            ax_plan.set_yticks(x_plan)
            ax_plan.set_yticklabels(plot_plan["plan_family"].astype(str).str[:50].tolist(), fontsize=9)
            ax_plan.set_xlabel("Median rate")
            ax_plan.set_title(f"Median rate by plan_family ({selected_payer})")
            fig_plan.tight_layout()
            buf_plan_png = io.BytesIO()
            fig_plan.savefig(buf_plan_png, format="png", dpi=150, bbox_inches="tight")
            buf_plan_png.seek(0)
            png_plan = buf_plan_png.getvalue()
            st.pyplot(fig_plan)
            plt.close(fig_plan)
            st.download_button("Download plan chart as PNG", data=png_plan, file_name="plan_comparison_chart.png", mime="image/png", key="dl_plan_png")
    elif selected_payer and selected_payer != "— Select —":
        st.caption("No plan-level data for this payer.")

else:
    if not filters_run:
        st.caption("Set filters above and click **Run comparison** to load data.")
