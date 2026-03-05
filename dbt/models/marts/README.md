## Marts

Thin consumption-layer models for downstream analytics and applications (including Streamlit). These will reference facts and dimensions, not raw sources.

### Payer/plan harmonization and comparability

Seeds (`seed_payer_map`, `seed_plan_map`) provide editable payer/plan family mapping and are built into the marts schema so `ref('seed_payer_map')` and `ref('seed_plan_map')` resolve. **You must run `dbt seed` before building payer harmonization models** (e.g. `dim_payer_harmonized`, `fct_rates_comparable_harmonized`).

```bash
dbt seed
dbt run --select dim_payer_harmonized fct_rates_comparable+ agg_hospital_procedure_compare
```

Or run only the comparability chain (after seeds and base marts exist):

```bash
dbt run --select dim_payer_harmonized fct_rates_comparable fct_rates_comparable_harmonized agg_hospital_procedure_compare
```

