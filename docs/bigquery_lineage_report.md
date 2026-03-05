# BigQuery lineage report and cleanup recommendations

**Project:** `pricing-transparency-portfolio`  
**Purpose:** Understand dependencies and recommend what can be dropped safely. **No automatic deletes.** Run inventory and lineage scripts manually, then use the cleanup plan only after you confirm.

---

## 1. Dependency graph (backwards from marts)

```
pt_analytics_marts.*
  ├── dim_hospital          ← int_standard_charges_base (intermediate), source(pt_analytics.pt_csv_registry)
  ├── dim_payer             ← int_standard_charges_base
  ├── dim_procedure         ← int_standard_charges_base
  ├── dim_source_file       ← source(pt_analytics.pt_csv_registry), source(pt_analytics.pt_json_registry)
  ├── fct_standard_charges  ← int_standard_charges_base, dim_hospital
  └── fct_standard_charges_semantic ← fct_standard_charges, int_standard_charges_base, dim_hospital

pt_analytics_intermediate.*
  ├── int_standard_charges_base  ← int_pt_standard_charges_union (BQ) or stg_silver_standard_charges (local only)
  └── int_pt_standard_charges_union ← stg_pt_json_rates, stg_pt_csv_tall, stg_pt_csv_wide

pt_analytics_staging.*
  ├── stg_pt_json_rates   ← source(pt_analytics.pt_json_extracted_rates)
  ├── stg_pt_csv_tall    ← source(pt_analytics.pt_csv_raw_tall), source(pt_analytics.pt_csv_registry)
  └── stg_pt_csv_wide    ← source(pt_analytics.pt_csv_raw_wide), source(pt_analytics.pt_csv_registry)

pt_analytics.*  (raw/landing – sources for dbt)
  ├── pt_csv_registry
  ├── pt_csv_raw_tall
  ├── pt_csv_raw_wide
  ├── pt_json_registry
  └── pt_json_extracted_rates
```

**Summary:** `pt_analytics_marts` depends on `pt_analytics_intermediate` and (for dim_source_file / dim_hospital) on `pt_analytics`. Intermediate depends on staging. Staging depends on `pt_analytics`. So the full chain is: **marts ← intermediate ← staging ← pt_analytics**.

---

## 2. Expected tables per dataset

Run **scripts/bq_inventory.sql** (separate query per dataset) to fill actual `table_type`, `last_modified_time`, `row_count`, `size_bytes`. Below is the expected structure from dbt and sources.

### pt_analytics_marts (required for Streamlit)

| Table | Type (dbt) | Referenced by app |
|-------|------------|--------------------|
| dim_hospital | table | Yes |
| dim_payer | table | Yes |
| dim_procedure | table | Yes |
| dim_source_file | table | Yes |
| fct_standard_charges | table | Yes (semantic reads from it) |
| fct_standard_charges_semantic | table | Yes |

### pt_analytics_intermediate (required to build marts)

| Table | Type (dbt) |
|-------|------------|
| int_standard_charges_base | table |
| int_pt_standard_charges_union | table |

### pt_analytics_staging (required to build intermediate)

| Table | Type (dbt) |
|-------|------------|
| stg_pt_json_rates | view |
| stg_pt_csv_tall | view |
| stg_pt_csv_wide | view |

(Local-only models `stg_silver_standard_charges`, `silver_standard_charges` are not in BigQuery.)

### pt_analytics (sources – required to build staging)

| Table | Purpose |
|-------|--------|
| pt_csv_registry | Registry for CSV loads |
| pt_json_registry | Registry for JSON loads |
| pt_csv_raw_tall | Raw CSV tall |
| pt_csv_raw_wide | Raw CSV wide |
| pt_json_extracted_rates | Extracted JSON rates |

---

## 3. Identifying likely artifacts

Use the inventory output and the rules below. **If unsure, mark “Do Not Touch Yet.”**

- **Not referenced by dbt**
  - Any table/view in a dataset that is not in the lists above and is not a known dbt model or source (e.g. old copies, one-off backfills).
- **Old with no downstream dependencies**
  - Tables with `last_modified_time` older than 90 days (or your chosen cutoff) that are not upstream of any current marts/intermediate/staging model. Cross-check with **scripts/bq_lineage.sql** and dbt ref/source list.
- **Duplicate of a newer table**
  - Same or very similar columns and purpose (e.g. `fct_standard_charges_backup`, `dim_hospital_old`). Compare names and schema; prefer keeping the one dbt/materialization uses.

---

## 4. Keep / Consider Dropping / Do Not Touch Yet

### Keep (required for app or rebuild)

| Dataset | Tables | Reason |
|--------|--------|--------|
| **pt_analytics_marts** | dim_hospital, dim_payer, dim_procedure, dim_source_file, fct_standard_charges, fct_standard_charges_semantic | **Minimum viable for Streamlit.** Do not drop. |
| **pt_analytics_intermediate** | int_standard_charges_base, int_pt_standard_charges_union | Needed to (re)build marts. |
| **pt_analytics_staging** | stg_pt_json_rates, stg_pt_csv_tall, stg_pt_csv_wide | Needed to (re)build intermediate. |
| **pt_analytics** | pt_csv_registry, pt_json_registry, pt_csv_raw_tall, pt_csv_raw_wide, pt_json_extracted_rates | Sources for staging; needed to (re)build pipeline. |

### Consider Dropping (manual only, after backup)

- **Extra tables in any dataset** that are not in the lists above (e.g. old snapshots, abandoned experiments). Only after you confirm they are not used by any job or downstream process.
- **Entire datasets** only if you have decided to retire that layer (e.g. you no longer need to rebuild from staging). Prefer dropping individual tables first.

### Do Not Touch Yet

- Anything you cannot clearly map to “not used by dbt or app.”
- **pt_analytics_marts** as a whole — do not delete the dataset; the app depends on it.
- Any table that might be referenced by scheduled jobs, BI tools, or scripts you are not sure about.

---

## 5. How to get actual table list and metrics

1. Run **scripts/bq_inventory.sql** (one section per dataset) to get for each table: `dataset_name`, `table_name`, `table_type`, `creation_time`, `last_modified_time`, `row_count` (or `estimated_rows`), `size_bytes`.
2. Run **scripts/bq_lineage.sql** (where applicable) to infer referenced tables from jobs or to use the dbt-based mapping.
3. Compare inventory output to the “Expected tables” in this report; flag tables that are not in the expected list as candidates for “Consider Dropping” after verification.

---

## 6. Minimum viable for the Streamlit app (summary)

- **Dataset:** `pt_analytics_marts`
- **Tables (all required):**
  - dim_hospital  
  - dim_payer  
  - dim_procedure  
  - dim_source_file  
  - fct_standard_charges  
  - fct_standard_charges_semantic  

To **rebuild** these marts from scratch you also need: `pt_analytics_intermediate`, `pt_analytics_staging`, and `pt_analytics` with the source tables listed above. For **cleanup**, keep everything in the “Keep” section unless you have a backup and a deliberate plan to drop specific artifacts; when in doubt, **Do Not Touch Yet.**

**Scripts:** Run **scripts/bq_inventory.sql** (Section 1 for table list per dataset; Section 3 for row_count/size_bytes via TABLE_STORAGE). Run **scripts/bq_lineage.sql** to compare with dbt-based lineage. Use **docs/bigquery_cleanup_plan.md** for manual backup and DROP snippets only after you decide what to remove.
