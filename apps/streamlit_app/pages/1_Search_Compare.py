"""
Search & Compare: hospital selector, procedure search, optional payer/plan filters, results table.
BigQuery-only for Cloud runs.
"""
import io
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

limit_options = [100, 250, 500, 1000, 2500, 5000]
default_limit = 500
max_limit = st.sidebar.selectbox("Max results", limit_options, index=limit_options.index(default_limit) if default_limit in limit_options else 2)

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

# Demo mode: run search with first hospital and demo procedure, store results
if st.session_state.get("demo_load"):
    st.session_state["demo_load"] = False
    hospital_id = str(hospital_ids[0])
    procs = data.search_procedures(ui.DEMO_PROCEDURE_QUERY, limit=5)
    billing_code = None
    if not procs.empty and "billing_code" in procs.columns:
        billing_code = str(procs.iloc[0]["billing_code"])
    with st.spinner("Loading demo results…"):
        st.session_state["search_results_df"] = data.get_rates(hospital_id, billing_code=billing_code, limit=ui.DEMO_RESULTS_LIMIT)
    st.session_state["search_demo_shown"] = True
    st.rerun()

# Form: always visible
sel_idx = st.selectbox("Hospital", range(len(hospital_labels)), format_func=lambda i: hospital_labels[i])
hospital_id = str(hospital_ids[sel_idx])

procedure_query = st.text_input("Procedure search (code or description)", placeholder="e.g. 99213 or office visit")
procedures = data.search_procedures(procedure_query, limit=50)
if not procedures.empty and procedure_query.strip():
    st.caption(f"Found {len(procedures)} matching procedures. Select one to filter, or leave blank for all.")
    proc_options = ["— All —"] + list(procedures["billing_code"].astype(str) + " | " + procedures["description"].astype(str).str[:60])
    proc_choice = st.selectbox("Filter by procedure", proc_options)
    billing_code = None if proc_choice == "— All —" else procedures.iloc[proc_options.index(proc_choice) - 1]["billing_code"]
else:
    billing_code = None

payers_df = data.load_dim_payer(data.get_mode())
payer_options = ["— Any —"]
if not payers_df.empty and "payer_name" in payers_df.columns:
    payer_options += list(payers_df["payer_name"].astype(str).drop_duplicates().head(200))
payer_name_sel = st.selectbox("Payer (optional)", payer_options)
payer_name = None if payer_name_sel == "— Any —" else payer_name_sel

plan_options = ["— Any —"]
if not payers_df.empty and "plan_name" in payers_df.columns and payer_name:
    plans = payers_df.loc[payers_df["payer_name"].astype(str) == str(payer_name), "plan_name"].drop_duplicates().head(100)
    plan_options += list(plans.astype(str))
plan_name_sel = st.selectbox("Plan (optional)", plan_options)
plan_name = None if plan_name_sel == "— Any —" else plan_name_sel

rate_cat_options = ["— Any —", "negotiated", "gross", "cash", "min", "max", "percentage", "other"]
rate_category_sel = st.selectbox("Rate category (optional)", rate_cat_options)
rate_category = None if rate_category_sel == "— Any —" else rate_category_sel

billing_type_options = ["— Any —", "CPT", "HCPCS", "NDC", "REVENUE", "ICD-10-PCS", "UNKNOWN"]
billing_code_type_sel = st.selectbox("Billing code type (optional)", billing_type_options)
billing_code_type = None if billing_code_type_sel == "— Any —" else billing_code_type_sel

run_clicked = st.button("Run search")

# Run search on button click
if run_clicked:
    with st.spinner("Loading rates…"):
        st.session_state["search_results_df"] = data.get_rates(
            hospital_id, billing_code=billing_code, payer_name=payer_name, plan_name=plan_name,
            rate_category=rate_category, billing_code_type=billing_code_type, limit=max_limit,
        )
    st.session_state["search_demo_shown"] = False
    st.rerun()

# Results section (from session state after demo or Run search)
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
            label="Download results (CSV)",
            data=buf.getvalue().encode("utf-8"),
            mime="text/csv",
            file_name="search_results.csv",
            key="download_search_csv",
        )
