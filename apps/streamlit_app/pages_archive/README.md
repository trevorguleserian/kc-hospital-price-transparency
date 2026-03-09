# Archived Streamlit Pages

These pages were removed from the main app navigation for the recruiter-facing portfolio experience. They are preserved here for reference or future re-enablement.

**Archived pages (in this folder):**

- `2_Hospital_Profile.py` — Hospital Profile (drill-down KPIs, top procedures, payer coverage).
- `3_Data_Quality.py` — Data Quality (null rates, coverage matrix, variant flags).
- `4_Hospital_Comparison.py` — Hospital Comparison (min/max/median by hospital for procedure and payer/plan).
- `5_Payer_Plan_Comparison.py` — Payer Plan Comparison (rates by payer_family and plan_family).
- `Top_Codes_By_Type.py` — Top Codes by Type (QA-style view of top billing codes).

*Note:* `1_Search_Compare.py` was removed from the app earlier; it can be recovered from git history if needed.

To restore a page to the app, move its file back into `pages/`. The app entrypoint is `Home.py`; Streamlit discovers pages in `pages/` by filename.

**Current live pages:** Home, Executive BI Dashboard (`2_Executive_BI_Dashboard.py`).
