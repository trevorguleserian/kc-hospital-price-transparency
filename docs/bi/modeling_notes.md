# BI Modeling Notes

Notes for building reports (e.g., in Power BI) from the exported fact and dimension tables.

---

## Star schema

The dataset is a star schema:

- **Fact:** `fct_standard_charges_semantic` — one row per charge-rate combination (hospital, procedure, payer, rate type, amount). All analysis and measures are built from this table.
- **Dimensions:** `dim_hospital`, `dim_payer`, `dim_procedure` — descriptive attributes. Filter and group by dimension columns; avoid storing measures in dimensions.

Dimensions connect to the fact via foreign keys. Slicers and filters on dimensions affect the fact through these relationships; measures aggregate the fact (e.g., sum of rate_amount, count of charge rows).

---

## Suggested Power BI relationships

Create these relationships so that filter context flows from dimensions to the fact:

| From (table)   | From column(s)     | To (table)   | To column(s)   | Cardinality | Cross filter |
|----------------|--------------------|--------------|----------------|-------------|--------------|
| fct_standard_charges_semantic | hospital_id        | dim_hospital | hospital_id    | Many to one | Single       |
| fct_standard_charges_semantic | billing_code       | dim_procedure| billing_code   | Many to one | Single       |
| fct_standard_charges_semantic | payer_name, plan_name | dim_payer | payer_name, plan_name | Many to one | Single       |

If your tool does not support composite keys (payer_name + plan_name), create a single surrogate key in dim_payer (e.g., a computed column or index) and add a matching key to the fact, then relate on that key.

For dim_procedure, if multiple rows share the same billing_code, use a composite key (billing_code + billing_code_type) in the dimension and in the fact, and relate on both columns (or on a single surrogate key derived from them).

---

## Suggested measures (DAX-friendly)

These are concise descriptions for measures you can implement in DAX (or equivalent). No DAX code is provided.

1. **Total charge amount** — Sum of `rate_amount` where `rate_unit` = 'dollars'; use for total listed or negotiated dollars.
2. **Distinct charge records** — Distinct count of `semantic_charge_sk`; number of charge-rate rows in scope.
3. **Average charge per procedure** — Sum of `rate_amount` (dollars) divided by distinct count of `billing_code` (or of semantic_charge_sk) in scope; average price per procedure.
4. **Negotiated share of charges** — Sum of `rate_amount` where `rate_category` = 'negotiated' divided by total sum of `rate_amount` (dollars); share of negotiated vs other types.
5. **Gross charge total** — Sum of `rate_amount` where `rate_category` = 'gross'; total gross charges in scope.
6. **Cash price total** — Sum of `rate_amount` where `rate_category` = 'cash'; total cash/discounted cash prices.
7. **Charge spread (min vs max)** — For a given procedure/hospital/payer, (max `rate_amount` where rate_category = 'max') minus (min `rate_amount` where rate_category = 'min'); range of listed prices.
8. **Procedure count** — Distinct count of `billing_code` (or of procedure key) in the fact; number of procedures with at least one charge.
9. **Hospital count** — Distinct count of `hospital_id` in the fact; number of hospitals with charges in scope.
10. **Payer charge total** — Sum of `rate_amount` (dollars) by payer (and optionally plan); use for payer-level comparisons and rankings.

When building these, restrict to rows where `rate_unit` = 'dollars' for monetary measures so that percentage-type rates are not summed as dollars.
