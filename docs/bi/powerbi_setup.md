# Power BI setup

Steps to connect Power BI Desktop to the BI exports and build a basic model.

## 1. Open Power BI Desktop

Launch Power BI Desktop. Use a new report or an existing one.

## 2. Load the CSV exports

- **Get Data** -> **Text/CSV**.
- Browse to the `dbt/exports/` folder (relative to the project root).
- Load each CSV:
  - `fct_standard_charges_semantic.csv`
  - `dim_hospital.csv`
  - `dim_payer.csv`
  - `dim_procedure.csv`
- Use **Load** or **Transform Data** to set data types (e.g. numeric for `rate_amount`, text for keys and names). Then close Power Query if you opened it.

## 3. Create relationships

In **Model** view, create these relationships (fact -> dimension, many-to-one, single filter direction from dimension to fact):

| From (fact) | From column(s) | To (dimension) | To column(s) |
|-------------|----------------|----------------|--------------|
| fct_standard_charges_semantic | hospital_id | dim_hospital | hospital_id |
| fct_standard_charges_semantic | billing_code | dim_procedure | billing_code |
| fct_standard_charges_semantic | payer_name, plan_name | dim_payer | payer_name, plan_name |

If your Power BI version does not support composite keys, add a surrogate key in the data (e.g. `payer_key` in dim_payer and a matching column in the fact) and create a single-column relationship from fact to dim_payer on that key.

## 4. Suggested measures

- **Total charge amount** — Sum of `rate_amount` where `rate_unit` = "dollars".
- **Distinct charge records** — Distinct count of `semantic_charge_sk`.
- **Average charge per procedure** — Sum of `rate_amount` (dollars) divided by distinct count of procedures in scope.
- **Negotiated share** — Sum of `rate_amount` where `rate_category` = "negotiated" divided by total sum of `rate_amount` (dollars).
- **Gross charge total** — Sum of `rate_amount` where `rate_category` = "gross".
- **Cash price total** — Sum of `rate_amount` where `rate_category` = "cash".
- **Charge spread** — Max `rate_amount` (rate_category = "max") minus min `rate_amount` (rate_category = "min") for the current context.
- **Procedure count** — Distinct count of `billing_code` in the fact.
- **Hospital count** — Distinct count of `hospital_id` in the fact.
- **Payer charge total** — Sum of `rate_amount` (dollars) by payer (and plan); for payer comparisons.

For all monetary measures, filter to `rate_unit` = "dollars" so percentage rates are not included in sums.
