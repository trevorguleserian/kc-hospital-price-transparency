"""
Search & Compare: comparable-only (recommended) or full semantic fact; procedure, payer/plan, rate filters; results table + CSV download.
BigQuery-only for Cloud runs.
"""
from __future__ import annotations

import io
from typing import Optional

import streamlit as st

from lib import data, ui
from lib import debug

st.set_page_config(page_title="Search & Compare", page_icon="🔍", layout="wide")
debug.require_bq_secrets_or_stop()
ui.render_sidebar()

ok, msg = data.ensure_data_available()
if not ok:
    st.error(msg)
    st.stop()

st.title("Search & Compare")

use_comparable = st.checkbox("Comparable-only (recommended)", value=True, help="Query comparable + harmonized rates (fct_rates_comparable_harmonized). When OFF, query full semantic fact with single-hospital filter.")

limit_options = [100, 250, 500, 1000, 2500, 5000]
default_limit = 500
max_limit = st.sidebar.selectbox("Max results", limit_options, index=limit_options.index(default_limit) if default_limit in limit_options else 2)

hospitals_df = data.load_dim_hospital(data.get_mode())
if hospitals_df.empty:
    st.warning("No hospitals in data.")
    st.stop()

if "hospital_id" in hospitals_df.columns:
    display_col = "hospital_name_clean" if "hospital_name_clean" in hospitals_df.columns else "hospital_name"
    if display_col not in hospitals_df.columns:
        display_col = hospitals_df.columns[1] if len(hospitals_df.columns) > 1 else hospitals_df.columns[0]
    opts = hospitals_df[["hospital_id", display_col]].drop_duplicates()
    hospital_options = list(opts.itertuples(index=False, name=None))
    hospital_labels = [f"{h[1]} ({h[0]})" for h in hospital_options]
    all_hospital_ids = [h[0] for h in hospital_options]
else:
    hospital_labels = list(hospitals_df.iloc[:, 0].astype(str))
    all_hospital_ids = hospital_labels
    hospital_options = [(hid, hid) for hid in all_hospital_ids]

# Payer/plan from dim_payer_harmonized for comparable mode and for consistent filters
payer_harmonized = data.load_dim_payer_harmonized(data.get_mode())
payer_families = ["— Any —"]
if not payer_harmonized.empty and "payer_family" in payer_harmonized.columns:
    payer_families += payer_harmonized["payer_family"].astype(str).drop_duplicates().sort_values().tolist()

# Demo mode
if st.session_state.get("demo_load"):
    st.session_state["demo_load"] = False
    if use_comparable:
        procs = data.search_procedures(ui.DEMO_PROCEDURE_QUERY, limit=5)
        billing_code = str(procs.iloc[0]["billing_code"]) if not procs.empty and "billing_code" in procs.columns else "99213"
        with st.spinner("Loading demo results…"):
            st.session_state["search_results_df"] = data.get_rates(
                billing_code=billing_code,
                rate_category="negotiated",
                rate_unit="dollars",
                use_comparable=True,
                limit=ui.DEMO_RESULTS_LIMIT,
            )
    else:
        hospital_id = str(all_hospital_ids[0])
        billing_code = "99213"
        with st.spinner("Loading demo results…"):
            st.session_state["search_results_df"] = data.get_rates(
                hospital_id=hospital_id,
                billing_code=billing_code,
                use_comparable=False,
                limit=ui.DEMO_RESULTS_LIMIT,
            )
    st.session_state["search_demo_shown"] = True
    st.rerun()

# ---------- Filters ----------
if use_comparable:
    st.caption("Comparable-only: code, rate category, and rate unit are required. Payer/plan family and hospitals are optional.")
    # Code (required): procedure search or direct input
    procedure_query = st.text_input("Procedure search (code or description)", placeholder="e.g. 99213 or office visit", key="proc_q")
    procedures = data.search_procedures(procedure_query, limit=50)
    billing_code: Optional[str] = None
    if procedures.empty or not (procedure_query or "").strip():
        billing_code = st.text_input("Or enter billing code directly (required)", placeholder="e.g. 99213", key="billing_direct").strip() or None
    else:
        st.caption(f"Found {len(procedures)} matching procedures. Select one (required).")
        proc_options = ["— Select —"] + list(procedures["billing_code"].astype(str) + " | " + procedures["description"].astype(str).str[:60])
        proc_choice = st.selectbox("Filter by procedure", proc_options, key="proc_sel")
        if proc_choice != "— Select —":
            billing_code = str(procedures.iloc[proc_options.index(proc_choice) - 1]["billing_code"])
        else:
            billing_code = st.text_input("Or enter billing code directly (required)", placeholder="e.g. 99213", key="billing_direct2").strip() or None

    rate_cat_options = ["negotiated", "gross", "cash", "min", "max", "percentage", "other"]
    rate_category = st.selectbox("Rate category (required)", rate_cat_options, key="rc_cat")
    rate_unit_options = ["dollars", "percent"]
    rate_unit = st.selectbox("Rate unit (required)", rate_unit_options, key="rc_unit")

    payer_family_sel = st.selectbox("Payer family (optional)", payer_families, key="pf_sel")
    payer_family: Optional[str] = None if payer_family_sel == "— Any —" else payer_family_sel
    plan_list = ["— Any —"]
    if payer_family and not payer_harmonized.empty and "plan_family" in payer_harmonized.columns:
        plans = payer_harmonized.loc[payer_harmonized["payer_family"].astype(str) == str(payer_family), "plan_family"]
        plan_list += plans.astype(str).drop_duplicates().sort_values().tolist()
    plan_family_sel = st.selectbox("Plan family (optional)", plan_list, key="plf_sel")
    plan_family = None if plan_family_sel == "— Any —" else plan_family_sel

    billing_code_type_options = ["— Any —", "CPT", "HCPCS", "NDC", "REVENUE", "ICD-10-PCS", "UNKNOWN"]
    billing_code_type_sel = st.selectbox("Billing code type (optional)", billing_code_type_options, key="bct_sel")
    billing_code_type = None if billing_code_type_sel == "— Any —" else billing_code_type_sel

    hospital_multiselect = st.multiselect(
        "Hospitals (optional; leave empty for all)",
        options=range(len(hospital_options)),
        format_func=lambda i: hospital_labels[i],
        default=[],
        key="hosp_mult",
    )
    selected_hospital_ids: Optional[list[str]] = [hospital_options[i][0] for i in hospital_multiselect] if hospital_multiselect else None
else:
    st.caption("Full semantic fact: select a hospital (required), then optional procedure and payer/plan filters.")
    sel_idx = st.selectbox("Hospital (required)", range(len(hospital_labels)), format_func=lambda i: hospital_labels[i], key="hosp_sel")
    hospital_id = str(all_hospital_ids[sel_idx])

    procedure_query = st.text_input("Procedure search (code or description)", placeholder="e.g. 99213 or office visit", key="proc_q2")
    procedures = data.search_procedures(procedure_query, limit=50)
    billing_code = None
    if not procedures.empty and (procedure_query or "").strip():
        proc_options = ["— All —"] + list(procedures["billing_code"].astype(str) + " | " + procedures["description"].astype(str).str[:60])
        proc_choice = st.selectbox("Filter by procedure", proc_options, key="proc_sel2")
        if proc_choice != "— All —":
            billing_code = str(procedures.iloc[proc_options.index(proc_choice) - 1]["billing_code"])

    payers_df = data.load_dim_payer(data.get_mode())
    payer_options = ["— Any —"]
    if not payers_df.empty and "payer_name" in payers_df.columns:
        payer_options += list(payers_df["payer_name"].astype(str).drop_duplicates().head(200))
    payer_name_sel = st.selectbox("Payer (optional)", payer_options, key="payer_sel")
    payer_name: Optional[str] = None if payer_name_sel == "— Any —" else payer_name_sel
    plan_options = ["— Any —"]
    if not payers_df.empty and "plan_name" in payers_df.columns and payer_name:
        plans = payers_df.loc[payers_df["payer_name"].astype(str) == str(payer_name), "plan_name"].drop_duplicates().head(100)
        plan_options += list(plans.astype(str))
    plan_name_sel = st.selectbox("Plan (optional)", plan_options, key="plan_sel")
    plan_name = None if plan_name_sel == "— Any —" else plan_name_sel

    rate_cat_options = ["— Any —", "negotiated", "gross", "cash", "min", "max", "percentage", "other"]
    rate_category_sel = st.selectbox("Rate category (optional)", rate_cat_options, key="rc_cat2")
    rate_category = None if rate_category_sel == "— Any —" else rate_category_sel

    billing_code_type_options = ["— Any —", "CPT", "HCPCS", "NDC", "REVENUE", "ICD-10-PCS", "UNKNOWN"]
    billing_code_type_sel = st.selectbox("Billing code type (optional)", billing_code_type_options, key="bct_sel2")
    billing_code_type = None if billing_code_type_sel == "— Any —" else billing_code_type_sel
    selected_hospital_ids = None

run_clicked = st.button("Run search")

if run_clicked:
    if use_comparable:
        if not billing_code or not rate_category or not rate_unit:
            st.error("Comparable-only requires: billing code, rate category, and rate unit.")
        else:
            with st.spinner("Loading rates…"):
                st.session_state["search_results_df"] = data.get_rates(
                    hospital_id=None,
                    hospital_ids=selected_hospital_ids,
                    billing_code=billing_code,
                    payer_family=payer_family,
                    plan_family=plan_family,
                    rate_category=rate_category,
                    rate_unit=rate_unit,
                    billing_code_type=billing_code_type,
                    use_comparable=True,
                    limit=max_limit,
                )
            st.session_state["search_demo_shown"] = False
            st.rerun()
    else:
        with st.spinner("Loading rates…"):
            st.session_state["search_results_df"] = data.get_rates(
                hospital_id=hospital_id,
                billing_code=billing_code,
                payer_name=payer_name,
                plan_name=plan_name,
                rate_category=rate_category,
                billing_code_type=billing_code_type,
                use_comparable=False,
                limit=max_limit,
            )
        st.session_state["search_demo_shown"] = False
        st.rerun()

# ---------- Results ----------
results_df = st.session_state.get("search_results_df")
if results_df is not None:
    st.divider()
    if st.session_state.get("search_demo_shown"):
        ui.render_demo_story_card()
        st.session_state["search_demo_shown"] = False
    with st.expander("Rate categories explained"):
        st.markdown("**negotiated** = payer-specific contracted rate. **gross** = list (cash) price. **cash** = discounted cash price. **min** / **max** = minimum or maximum of a rate range.")
    if results_df.empty:
        st.info("No rates found for this selection.")
    else:
        st.dataframe(results_df, use_container_width=True)
        st.caption(f"Showing up to {len(results_df)} rows.")
        buf = io.StringIO()
        results_df.to_csv(buf, index=False)
        st.download_button(
            label="Download results CSV",
            data=buf.getvalue().encode("utf-8"),
            mime="text/csv",
            file_name="search_results.csv",
            key="download_search_csv",
        )
