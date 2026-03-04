# BI Data Dictionary

Final tables exported for reporting: one fact table and three dimension tables. Use with Power BI, Tableau, or other tools against the star schema described in `modeling_notes.md`.

---

## fct_standard_charges_semantic

**Grain:** One row per charge record and rate type: (standard_charge_sk, rate_category). A single source charge can appear as multiple rows (e.g., negotiated, gross, cash, min, max).

**Primary key:** `semantic_charge_sk`

**Join keys:** `hospital_id` -> dim_hospital; `billing_code` (and optionally `billing_code_type`) -> dim_procedure; `payer_name`, `plan_name` -> dim_payer

| Column | Business meaning |
|--------|------------------|
| semantic_charge_sk | Surrogate key for this fact row; use for distinct count of charge-rate combinations. |
| standard_charge_sk | Links back to the original charge record; multiple semantic rows can share this. |
| hospital_id | Hospital that posted the charge; join to dim_hospital for facility attributes. |
| hospital_name | Denormalized hospital name from source; prefer dim_hospital.hospital_name_clean in reports. |
| billing_code | Procedure/service code (CPT, HCPCS, DRG, etc.); join to dim_procedure. |
| billing_code_type | Code system (e.g., CPT, HCPCS, DRG, NDC). |
| description | Procedure or service description from the charge. |
| payer_name | Payer name when present; join with plan_name to dim_payer. |
| plan_name | Plan name when present; join with payer_name to dim_payer. |
| rate_category | Canonical rate type: negotiated, gross, cash, min, max, percentage, other. |
| rate_amount | Numeric price; interpretation depends on rate_unit. |
| rate_unit | Either "dollars" or "percent"; use for correct aggregation and labels. |
| source_system | Origin system: json, csv_tall, or csv_wide. |
| source_file_name | Source file for lineage and debugging. |
| ingested_at | When the record was loaded; useful for incremental or recency filters. |
| contracting_method | Reserved for future use (e.g., fee-for-service vs bundled). |

---

## dim_hospital

**Grain:** One row per hospital (or per source file in full build); identifies each facility that contributes standard charges.

**Primary key:** `hospital_id`

**Join keys:** Fact joins to this table on `hospital_id`.

| Column | Business meaning |
|--------|------------------|
| hospital_id | Stable surrogate key; use as the sole join key from the fact. |
| hospital_name | Raw or derived name from registry or file; may contain codes or formatting. |
| hospital_name_clean | Display name for reports: cleaned spaces, punctuation; use in slicers and labels. |
| hospital_name_norm | Normalized form for matching (lowercase, no punctuation); used to generate hospital_id. |
| source_system | Origin: json, csv_tall, or csv_wide. |
| source_file_name | Representative source file for lineage. |

---

## dim_payer

**Grain:** One row per distinct (payer_name, plan_name) combination present in the charge data.

**Primary key:** Composite (`payer_name`, `plan_name`)

**Join keys:** Fact joins on `payer_name` and `plan_name` together; both can be empty in the fact.

| Column | Business meaning |
|--------|------------------|
| payer_name | Payer or insurer name (e.g., Medicare, commercial plan). |
| plan_name | Plan or product name; empty string when not reported. |

**Note:** The dimension is schema-ready even when source files do not contain payer/plan fields (e.g., flat JSON or CSV without negotiated-by-payer detail). In that case `dim_payer` will have 0 rows until sources that include payer data (e.g., CMS-style JSON with `payers_information`) are ingested and the Silver layer is rebuilt.

---

## dim_procedure

**Grain:** One row per distinct (billing_code, billing_code_type, description) from the charge data.

**Primary key:** Composite (`billing_code`, `billing_code_type`); description can vary by source.

**Join keys:** Fact joins on `billing_code`; use `billing_code_type` as well when matching to a single procedure row.

| Column | Business meaning |
|--------|------------------|
| billing_code | Procedure or service code (CPT, HCPCS, DRG, NDC, etc.). |
| billing_code_type | Code system or type. |
| description | Human-readable procedure or service description. |
