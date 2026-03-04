"""
Streamlit app for hospital price transparency BI exports.
Reads parquet files from dbt/exports via DuckDB. Run scripts/run_local_bi.ps1 first if files are missing.
"""

from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Exports live at repo_root/dbt/exports; app is at repo_root/apps/streamlit_app/app.py
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
EXPORTS_DIR = REPO_ROOT / "dbt" / "exports"

REQUIRED_FILES = [
    "fct_standard_charges_semantic.parquet",
    "dim_hospital.parquet",
    "dim_procedure.parquet",
    "dim_payer.parquet",
]


def exports_available() -> bool:
    return all((EXPORTS_DIR / f).exists() for f in REQUIRED_FILES)


def _exports_path() -> str:
    """Return exports dir as string for cache key stability."""
    return str(EXPORTS_DIR.resolve())


@st.cache_data
def load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load BI parquet exports via DuckDB. Returns (charges, hospitals, procedures, payers)."""
    con = duckdb.connect()
    charges = con.execute(
        "SELECT * FROM read_parquet(?)",
        [str(EXPORTS_DIR / "fct_standard_charges_semantic.parquet")],
    ).df()
    hospitals = con.execute(
        "SELECT * FROM read_parquet(?)",
        [str(EXPORTS_DIR / "dim_hospital.parquet")],
    ).df()
    procedures = con.execute(
        "SELECT * FROM read_parquet(?)",
        [str(EXPORTS_DIR / "dim_procedure.parquet")],
    ).df()
    payers = con.execute(
        "SELECT * FROM read_parquet(?)",
        [str(EXPORTS_DIR / "dim_payer.parquet")],
    ).df()
    con.close()
    return charges, hospitals, procedures, payers


@st.cache_data
def get_data_health(_exports_dir: str) -> dict:
    """Compute data health metrics via DuckDB. Cached by exports path."""
    c = str(EXPORTS_DIR / "fct_standard_charges_semantic.parquet")
    h = str(EXPORTS_DIR / "dim_hospital.parquet")
    pc = str(EXPORTS_DIR / "dim_procedure.parquet")
    py = str(EXPORTS_DIR / "dim_payer.parquet")
    con = duckdb.connect()
    # Row counts
    charges_n = con.execute("SELECT count(*) FROM read_parquet(?)", [c]).fetchone()[0]
    hospitals_n = con.execute("SELECT count(*) FROM read_parquet(?)", [h]).fetchone()[0]
    procedures_n = con.execute("SELECT count(*) FROM read_parquet(?)", [pc]).fetchone()[0]
    payers_n = con.execute("SELECT count(*) FROM read_parquet(?)", [py]).fetchone()[0]
    # From charges: min/max ingested_at, distinct counts, null pcts
    q = """
    SELECT
        min(ingested_at) AS min_ingested_at,
        max(ingested_at) AS max_ingested_at,
        count(DISTINCT hospital_id) AS distinct_hospital_id,
        count(DISTINCT billing_code) AS distinct_billing_code,
        count(*) AS n,
        count(*) FILTER (WHERE hospital_id IS NULL) AS null_hospital_id,
        count(*) FILTER (WHERE billing_code IS NULL) AS null_billing_code,
        count(*) FILTER (WHERE rate_amount IS NULL) AS null_rate_amount,
        count(*) FILTER (WHERE description IS NULL) AS null_description
    FROM read_parquet(?)
    """
    row = con.execute(q, [c]).fetchone()
    con.close()
    n = row[4] or 0
    return {
        "charges_rows": charges_n,
        "hospitals_rows": hospitals_n,
        "procedures_rows": procedures_n,
        "payers_rows": payers_n,
        "min_ingested_at": row[0],
        "max_ingested_at": row[1],
        "distinct_hospital_id": row[2] or 0,
        "distinct_billing_code": row[3] or 0,
        "pct_null_hospital_id": (100.0 * (row[5] or 0) / n) if n else 0,
        "pct_null_billing_code": (100.0 * (row[6] or 0) / n) if n else 0,
        "pct_null_rate_amount": (100.0 * (row[7] or 0) / n) if n else 0,
        "pct_null_description": (100.0 * (row[8] or 0) / n) if n else 0,
    }


@st.cache_data
def get_hospital_metrics(_exports_dir: str, hospital_name: str) -> dict | None:
    """DuckDB aggregations for one hospital. Returns None if no rows."""
    con = duckdb.connect()
    q = """
    SELECT
        count(*) AS total_rows,
        count(DISTINCT billing_code) AS distinct_billing_code,
        median(rate_amount) AS median_rate_amount,
        min(rate_amount) AS min_rate_amount,
        max(rate_amount) AS max_rate_amount
    FROM read_parquet(?)
    WHERE hospital_name = ?
    """
    out = con.execute(q, [str(EXPORTS_DIR / "fct_standard_charges_semantic.parquet"), hospital_name]).fetchone()
    con.close()
    if out is None or out[0] == 0:
        return None
    return {
        "total_rows": out[0],
        "distinct_billing_code": out[1],
        "median_rate_amount": out[2],
        "min_rate_amount": out[3],
        "max_rate_amount": out[4],
    }


@st.cache_data
def get_hospital_top50(_exports_dir: str, hospital_name: str) -> pd.DataFrame:
    """Top 50 rows by rate_amount for one hospital via DuckDB."""
    con = duckdb.connect()
    q = """
    SELECT hospital_name, billing_code, billing_code_type, description, rate_amount, rate_category, ingested_at
    FROM read_parquet(?)
    WHERE hospital_name = ?
    ORDER BY rate_amount DESC NULLS LAST
    LIMIT 50
    """
    df = con.execute(q, [str(EXPORTS_DIR / "fct_standard_charges_semantic.parquet"), hospital_name]).df()
    con.close()
    return df


def load_and_join() -> pd.DataFrame:
    """Join fact to dims for display. Uses cached load_data()."""
    charges, dim_h, dim_pc, dim_py = load_data()
    fact = charges.copy()

    # Join for display fields: hospital_name_clean, procedure description, payer
    fact = fact.merge(
        dim_h[["hospital_id", "hospital_name_clean"]],
        on="hospital_id",
        how="left",
        suffixes=("", "_dim"),
    )
    fact = fact.rename(columns={"hospital_name_clean": "hospital_name_display"})

    fact = fact.merge(
        dim_pc[["billing_code", "billing_code_type", "description"]],
        on=["billing_code", "billing_code_type"],
        how="left",
        suffixes=("", "_proc"),
    )
    fact["procedure_description"] = fact["description_proc"].fillna(fact["description"])

    fact["payer_display"] = (
        fact["payer_name"].fillna("").astype(str)
        + " | "
        + fact["plan_name"].fillna("").astype(str)
    ).str.strip(" |")

    return fact


def _render_main_tab(
    _charges: pd.DataFrame,
    _hospitals: pd.DataFrame,
    _procedures: pd.DataFrame,
    _payers: pd.DataFrame,
) -> None:
    """Main dashboard: filters, KPIs, table, charts."""
    df = load_and_join()

    st.sidebar.header("Filters")
    hospital_options = sorted(df["hospital_name_display"].dropna().unique().tolist())
    selected_hospitals = st.sidebar.multiselect(
        "Hospital",
        options=hospital_options,
        default=[],
        placeholder="All hospitals",
    )
    rate_cats = sorted(df["rate_category"].dropna().unique().tolist())
    selected_rate_cats = st.sidebar.multiselect(
        "Rate category",
        options=rate_cats,
        default=[],
        placeholder="All categories",
    )
    billing_search = st.sidebar.text_input(
        "Billing code search",
        placeholder="e.g. 99213 or CPT",
    )
    payer_options = sorted(df["payer_display"].dropna().unique().tolist())
    selected_payers = st.sidebar.multiselect(
        "Payer",
        options=payer_options,
        default=[],
        placeholder="All payers",
    )

    mask = pd.Series(True, index=df.index)
    if selected_hospitals:
        mask &= df["hospital_name_display"].isin(selected_hospitals)
    if selected_rate_cats:
        mask &= df["rate_category"].isin(selected_rate_cats)
    if billing_search:
        search = billing_search.strip().lower()
        mask &= (
            df["billing_code"].astype(str).str.lower().str.contains(search, na=False)
            | df["billing_code_type"].astype(str).str.lower().str.contains(search, na=False)
            | df["procedure_description"].astype(str).str.lower().str.contains(search, na=False)
        )
    if selected_payers:
        mask &= df["payer_display"].isin(selected_payers)

    filtered = df.loc[mask].copy()

    row_count = len(filtered)
    distinct_hospitals = filtered["hospital_id"].nunique()
    distinct_codes = filtered["billing_code"].nunique()
    negotiated = filtered[filtered["rate_category"] == "negotiated"]
    median_negotiated = (
        negotiated["rate_amount"].median()
        if len(negotiated) > 0 and negotiated["rate_amount"].notna().any()
        else None
    )

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Rows", f"{row_count:,}")
    with col2:
        st.metric("Distinct hospitals", f"{distinct_hospitals:,}")
    with col3:
        st.metric("Distinct billing codes", f"{distinct_codes:,}")
    with col4:
        st.metric(
            "Median rate (negotiated)",
            f"${median_negotiated:,.2f}" if median_negotiated is not None else "—",
        )

    st.subheader("Filtered data")
    if row_count == 0:
        st.write("No rows match the current filters.")
        n_rows = 0
    else:
        max_val = min(5000, row_count)
        min_val = min(100, max_val)
        default_val = min(1000, max_val)
        n_rows = st.slider(
            "Rows to show",
            min_value=min_val,
            max_value=max_val,
            value=default_val,
            step=100 if max_val >= 100 else 1,
        )
    if n_rows > 0:
        display_cols = [
            "hospital_name_display",
            "billing_code",
            "billing_code_type",
            "procedure_description",
            "payer_display",
            "rate_category",
            "rate_amount",
            "rate_unit",
        ]
        table_df = filtered[display_cols].head(n_rows)
        table_df = table_df.rename(columns={
            "hospital_name_display": "Hospital",
            "procedure_description": "Procedure",
            "payer_display": "Payer",
            "rate_category": "Rate category",
            "rate_amount": "Rate amount",
            "rate_unit": "Rate unit",
        })
        st.dataframe(table_df, use_container_width=True, height=400)

    st.subheader("Charts")
    chart_col1, chart_col2 = st.columns(2)
    with chart_col1:
        neg = filtered[
            (filtered["rate_category"] == "negotiated")
            & (filtered["rate_unit"] == "dollars")
            & filtered["rate_amount"].notna()
        ]
        if len(neg) > 0:
            by_proc = (
                neg.groupby(["billing_code", "procedure_description"], dropna=False)
                ["rate_amount"]
                .median()
                .reset_index()
            )
            by_proc["procedure_label"] = (
                by_proc["billing_code"].astype(str) + " – " + by_proc["procedure_description"].fillna("").astype(str).str[:40]
            )
            top10 = by_proc.nlargest(10, "rate_amount")
            fig1 = px.bar(
                top10,
                x="rate_amount",
                y="procedure_label",
                orientation="h",
                title="Top 10 procedures by median negotiated rate",
                labels={"rate_amount": "Median rate ($)", "procedure_label": "Procedure"},
            )
            fig1.update_layout(yaxis=dict(autorange="reversed"), height=400)
            st.plotly_chart(fig1, use_container_width=True)
        else:
            st.info("No negotiated dollar rates in filtered data for this chart.")
    with chart_col2:
        dollars = filtered[
            (filtered["rate_unit"] == "dollars") & filtered["rate_amount"].notna()
        ]
        if len(dollars) > 0:
            fig2 = px.histogram(
                dollars,
                x="rate_amount",
                nbins=50,
                title="Rate amount distribution (dollars)",
                labels={"rate_amount": "Rate amount ($)", "count": "Count"},
            )
            fig2.update_layout(height=400)
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No dollar rates in filtered data for this chart.")


def _render_hospital_explorer() -> None:
    """Hospital Explorer tab: select hospital, KPIs, top 50 by rate_amount."""
    charges, _, _, _ = load_data()
    hospital_names = sorted(charges["hospital_name"].dropna().astype(str).unique().tolist())
    if not hospital_names:
        st.info("No hospital names in charges data.")
        return

    search = st.text_input("Filter hospitals", placeholder="Type to search...")
    if search:
        search_lower = search.strip().lower()
        hospital_names = [h for h in hospital_names if search_lower in h.lower()]
    if not hospital_names:
        st.warning("No hospitals match the filter.")
        return

    selected = st.selectbox("Select hospital", options=hospital_names, key="hospital_explorer_select")

    if not selected:
        return

    metrics = get_hospital_metrics(_exports_path(), selected)
    if metrics is None:
        st.warning(f"No rows for hospital: {selected}")
        return

    st.subheader("KPIs")
    k1, k2, k3, k4, k5 = st.columns(5)
    with k1:
        st.metric("Total rows", f"{metrics['total_rows']:,}")
    with k2:
        st.metric("Distinct billing codes", f"{metrics['distinct_billing_code']:,}")
    with k3:
        st.metric("Median rate", f"${metrics['median_rate_amount']:,.2f}" if metrics['median_rate_amount'] is not None else "—")
    with k4:
        st.metric("Min rate", f"${metrics['min_rate_amount']:,.2f}" if metrics['min_rate_amount'] is not None else "—")
    with k5:
        st.metric("Max rate", f"${metrics['max_rate_amount']:,.2f}" if metrics['max_rate_amount'] is not None else "—")

    st.subheader("Top 50 by rate_amount")
    top50 = get_hospital_top50(_exports_path(), selected)
    if top50.empty:
        st.write("No rows.")
    else:
        st.dataframe(top50, use_container_width=True, height=400)


def main() -> None:
    st.set_page_config(page_title="Hospital price transparency", layout="wide")
    st.title("Hospital price transparency")

    if not exports_available():
        st.warning(
            "Parquet exports not found. Run scripts/run_local_bi.ps1 first."
        )
        return

    charges, hospitals, procedures, payers = load_data()
    health = get_data_health(_exports_path())

    with st.expander("Data Health", expanded=True):
        st.caption("Row counts and quality metrics from dbt/exports (DuckDB).")
        r1, r2, r3, r4 = st.columns(4)
        with r1:
            st.metric("Charges", f"{health['charges_rows']:,} rows")
        with r2:
            st.metric("Hospitals", f"{health['hospitals_rows']:,} rows")
        with r3:
            st.metric("Procedures", f"{health['procedures_rows']:,} rows")
        with r4:
            st.metric("Payers", f"{health['payers_rows']:,} rows")
        st.write("**Charges:**")
        min_ts = health["min_ingested_at"]
        max_ts = health["max_ingested_at"]
        st.write(
            f"ingested_at: {min_ts if min_ts is not None else '—'} → {max_ts if max_ts is not None else '—'}  \n"
            f"Distinct hospital_id: {health['distinct_hospital_id']:,}  \n"
            f"Distinct billing_code: {health['distinct_billing_code']:,}"
        )
        st.write("**Null % (charges):**")
        nc1, nc2, nc3, nc4 = st.columns(4)
        with nc1:
            st.metric("hospital_id", f"{health['pct_null_hospital_id']:.2f}%")
        with nc2:
            st.metric("billing_code", f"{health['pct_null_billing_code']:.2f}%")
        with nc3:
            st.metric("rate_amount", f"{health['pct_null_rate_amount']:.2f}%")
        with nc4:
            st.metric("description", f"{health['pct_null_description']:.2f}%")

    tab_main, tab_explorer = st.tabs(["Main", "Hospital Explorer"])

    with tab_main:
        _render_main_tab(charges, hospitals, procedures, payers)

    with tab_explorer:
        _render_hospital_explorer()


if __name__ == "__main__":
    main()
