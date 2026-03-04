# Price Transparency Semantic Layer — Metric Definitions

This document defines business meanings for the semantic layer and recommended metrics for BI.

## Rate categories (rate_category)

| Category    | Meaning | Source fields |
|------------|---------|----------------|
| **negotiated** | Payer-negotiated rate (dollar amount) | `rate_amount` (rate_type: negotiated_rate, negotiated_dollar), `negotiated_dollar` (wide) |
| **gross**      | Gross charge / list price | `gross_charge`, or primary rate when rate_type is gross |
| **cash**       | Discounted cash / self-pay price | `discounted_cash`, or primary rate when rate_type is cash |
| **min**        | Minimum rate (e.g. range) | `minimum` |
| **max**        | Maximum rate (e.g. range) | `maximum` |
| **percentage** | Percent-based rate (e.g. of charges) | Primary rate when rate_type is negotiated_percentage / percentage |
| **other**      | Any other rate type not above | Fallback |

## Business definitions

- **Negotiated rate**: Amount a payer has contracted to pay for a service (per procedure/DRG/CPT).
- **Gross charge**: Published list price before discounts; often the highest price.
- **Cash price**: Price for uninsured or self-pay patients; may be lower than gross.
- **Min / max**: Bounds of a rate range when the source provides a span.
- **Percent-based rate**: Rate expressed as a percentage (e.g. of charges); use with `rate_unit = 'percent'`.

## Grain and duplicates

- **Grain**: `fct_standard_charges_semantic` is **one row per (standard_charge_sk, rate_category)**.
- **Duplicates**: The same charge record (standard_charge_sk) can appear in multiple rows with different `rate_category` (e.g. one row for negotiated, one for gross). Within (standard_charge_sk, rate_category) we keep a single row (latest `ingested_at` when deduping).
- **Nulls / zeros**: Rows are only emitted where `rate_amount` is not null and not zero.

## Recommended default metrics

1. **Median negotiated rate** (by hospital, procedure, payer):  
   `PERCENTILE_CONT(rate_amount, 0.5) WHERE rate_category = 'negotiated'`

2. **Average cash price** (by hospital, procedure):  
   `AVG(rate_amount) WHERE rate_category = 'cash'`

3. **Negotiated vs cash delta** (by hospital, procedure):  
   Difference between median negotiated and avg cash for the same (hospital_id, billing_code); use semantic fact filtered by rate_category.

## Usage in BI

- Filter by `rate_category` and `source_system` for consistent metric definitions.
- Join to `dim_hospital` (hospital_id), `dim_procedure` (billing_code), and `dim_source_file` (source_file_name, source_system) for dimensions and lineage.
- Use `rate_unit` to separate dollar vs percent metrics when building reports.
