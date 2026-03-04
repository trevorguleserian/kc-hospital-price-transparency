## Canonical Contract Data Contract

### Canonical Grain

The canonical hospital pricing contract is modeled at the following grain:

- **One row per**:
  - `facility_id`
  - `billing_code` (e.g. CPT/HCPCS/DRG or internal charge code)
  - `billing_code_type`
  - `payer_id`
  - `plan_id` (or most granular available plan identifier)
  - `billing_class` (e.g. inpatient, outpatient, professional, facility)
  - `payment_methodology`
  - `payment_unit`
  - `price_type`
  - `effective_start_date`
  - `effective_end_date`

This grain is designed to be **format-agnostic** across JSON, tall CSV, and wide CSV inputs.

### Comparability Rule

Rates are only considered comparable when all of the following canonical attributes are equal:

- `payment_methodology`
- `payment_unit`
- `price_type`

Analyses that compare or rank prices **must** enforce this rule (e.g. when computing relative price indices or distribution statistics).

### Required Canonical Columns (No SQL)

The following columns are required in the canonical contract model. Format-specific staging models are responsible for mapping raw fields into these canonical columns, including type casting and normalization.

- **Facility & Service Identification**
  - `facility_id` – Stable identifier for the facility or reporting entity.
  - `facility_name` – Human-readable facility name.
  - `billing_code` – Service or item identifier (CPT/HCPCS/DRG/charge code, etc.).
  - `billing_code_type` – Code system or type (e.g. `CPT`, `HCPCS`, `DRG`, `REV`, `CDM`).
  - `billing_code_description` – Description as provided in the source file.
  - `billing_class` – Encounter/billing class (e.g. `inpatient`, `outpatient`, `professional`, `facility`).

- **Payer & Plan Identification**
  - `payer_id` – Stable identifier for payer or carrier (normalized across files).
  - `payer_name` – Human-readable payer name.
  - `plan_id` – Identifier for the specific plan or product, where available.
  - `plan_name` – Human-readable plan name.
  - `network_tier` – Network tier or product tier (e.g. `in_network`, `out_of_network`, `tier_1`).

- **Payment Methodology & Units**
  - `payment_methodology` – High-level method (e.g. `fee_for_service`, `per_diem`, `case_rate`, `percent_of_charge`, `capitated`).
  - `payment_unit` – Unit of payment (e.g. `per_unit`, `per_visit`, `per_day`, `per_case`, `percent`).
  - `price_type` – Type of rate (e.g. `negotiated`, `cash`, `list`, `min`, `max`, `derived`).

- **Rate & Currency**
  - `negotiated_rate` – Numeric rate value in the smallest meaningful unit for the methodology.
  - `negotiated_rate_min` – Optional lower bound when ranges are provided.
  - `negotiated_rate_max` – Optional upper bound when ranges are provided.
  - `currency` – ISO 4217 currency code (e.g. `USD`).
  - `billing_multiplier` – Optional multiplier or quantity associated with the rate (e.g. RVUs or unit counts).

- **Contract & Temporal Attributes**
  - `modifier_1` – First procedure modifier, if applicable.
  - `modifier_2` – Second procedure modifier, if applicable.
  - `place_of_service` – Place of service or site of care, if available.
  - `contract_id` – Identifier for the underlying contract, when present.
  - `contract_name` – Human-readable contract name or label, when present.
  - `effective_start_date` – Date the rate becomes effective.
  - `effective_end_date` – Date the rate expires or is superseded.

- **Source & Lineage**
  - `source_file_name` – Original file name as ingested.
  - `source_format` – One of `json`, `csv_tall`, `csv_wide`.
  - `source_row_id` – Stable row-level identifier or hash for deduplication and lineage.
  - `ingested_at` – Timestamp when the record was first ingested.

- **Quality & Normalization**
  - `normalization_status` – Status flag (e.g. `standardized`, `partially_standardized`, `raw_passthrough`).
  - `data_quality_issues` – Optional free-text or structured indicator of notable data quality issues (e.g. missing codes, invalid dates).

Additional optional columns can be added later as needed, but **all canonical models and downstream fact tables must conform to this required base set** to ensure cross-format comparability and consistent contract analysis.

