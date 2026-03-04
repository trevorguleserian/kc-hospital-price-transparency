# Data Quality

This document summarizes how data quality is enforced: Silver-layer quarantine, dbt tests, and known limitations.

---

## Pipeline view

```
  Bronze  -->  Silver (good rows)  -->  dbt Gold  -->  BI / Streamlit
                |
                +-->  Silver quarantine (bad rows; reason_code)
```

Only rows that pass Silver validation are written to `standard_charges/` and flow into dbt. Quarantined rows are stored for inspection and diagnostics; they do not enter the star schema.

---

## Quarantine (Silver layer)

Invalid rows are written to `lake/silver/quarantine/ingest_date=YYYY-MM-DD/data.parquet`. Each row has a **reason_code** explaining why it was excluded.

| reason_code | Meaning |
|-------------|---------|
| **MISSING_CODE** | No billing code (null or blank). |
| **MISSING_RATE** | No rate amount; required for a charge row. |
| **BAD_RATE_VALUE** | Rate is null, non-numeric, or negative. |
| **missing_source_file_name** | Source file identifier missing (needed for lineage). |
| **JSON_UNSUPPORTED_SHAPE** | JSON file does not contain a recognized charge list (e.g. no `standardCharges` / `standard_charge_information`). |

A DQ gate (e.g. in orchestration) can fail the pipeline when **quarantine_rate** exceeds a threshold (e.g. 0.5), so that bad data does not dominate the lake.

---

## dbt tests

Tests run as part of `dbt build` (or `dbt test`). They guard referential integrity, nullability, and allowed values in the marts.

### Marts (schema: marts)

| Model | Column / scope | Test(s) |
|-------|----------------|--------|
| **dim_hospital** | hospital_id, hospital_name_clean, hospital_name_norm | not_null |
| **dim_payer** | — | (none) |
| **dim_procedure** | billing_code_type | accepted_values: CPT, HCPCS, NDC, REVENUE, ICD-10-PCS, UNKNOWN |
| **fct_standard_charges** | standard_charge_sk | not_null, unique |
| | hospital_id | not_null |
| | source_system | accepted_values: json, csv_tall, csv_wide |
| **fct_standard_charges_semantic** | semantic_charge_sk | not_null, unique |
| | standard_charge_sk, hospital_id, billing_code, ingested_at, source_system | not_null |
| | hospital_id | relationships → dim_hospital.hospital_id |
| | billing_code | relationships → dim_procedure.billing_code |
| | rate_category | accepted_values: negotiated, gross, cash, min, max, percentage, other |
| | source_system | accepted_values: json, csv_tall, csv_wide |
| **dim_source_file** | — | (none) |

### Singular test (warn-only)

| Test | Purpose | Severity |
|------|---------|----------|
| **assert_dim_procedure_pct_unknown_not_high** | Warns when ≥95% of `dim_procedure` rows have `billing_code_type = 'UNKNOWN'`. Heuristics are expected to reduce UNKNOWN; this flags regressions. | warn |

### Intermediate

| Model | Test |
|-------|------|
| **int_pt_standard_charges_union** | unique on standard_charge_sk |

Staging models do not define column tests in this project; quality is enforced at Silver (quarantine) and marts (tests above).

---

## Known limitations

- **Source variety:** Hospital files vary widely (CSV preamble rows, JSON shapes). Bronze and Silver handle a defined set of patterns; unrecognized formats may land in Bronze but yield no or few Silver rows, or high quarantine rates.
- **CSV header detection:** Some CSVs use row 3 (1-based) as the charge table header. Bronze scores candidate header rows; misdetection can produce zero or incorrect Silver rows. Force re-ingest and rebuild Silver if needed (see RUNBOOK).
- **Billing code types:** Inference (CPT, HCPCS, NDC, REVENUE, ICD-10-PCS) is heuristic. Codes that do not match these patterns remain UNKNOWN. The semantic model excludes null `billing_code`; it does not exclude UNKNOWN type.
- **Rate semantics:** Silver and dbt normalize rate types (e.g. negotiated, gross, cash) where possible. Hospital-specific labels may map to “other” or a generic category.
- **No row-level dedup in Silver:** Deduplication of equivalent rates is handled in dbt (e.g. surrogate keys, distinct in dimensions). Silver is one row per extracted rate from each source row.
- **Local vs BigQuery:** Test and export behavior are aligned where possible; some DuckDB vs BigQuery differences may exist in edge cases (e.g. regex, types).

For runbooks and troubleshooting, see [RUNBOOK.md](../RUNBOOK.md) and [docs/runbook.md](runbook.md).
