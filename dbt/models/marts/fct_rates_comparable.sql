{{
  config(
    materialized='table',
    schema='marts',
    tags=['local'],
  )
}}

/*
  Data contract: fct_rates_comparable
  -----------------------------------
  All rows from semantic with non-null rate_category and numeric rate_amount.
  rate_category is normalized (LOWER(TRIM)). is_comparable = TRUE for
  negotiated, gross, cash, min, max, percentage (rate_unit not required).
  is_comparable = FALSE for other. comparability_reason: comparable | excluded_other | excluded_unexpected_category.
  comparability_key: concat(billing_code_type, '|', rate_category_norm, '|', coalesce(rate_unit_norm, '')).
*/
{% set accepted_rate_categories = var('accepted_rate_categories', ['negotiated', 'gross', 'cash', 'min', 'max', 'percentage', 'other']) %}
{% set comparable_rate_categories = var('comparable_rate_categories', ['negotiated', 'gross', 'cash', 'min', 'max', 'percentage']) %}

with semantic as (
  select
    semantic_charge_sk,
    standard_charge_sk,
    hospital_id,
    hospital_name,
    coalesce(billing_code_raw, billing_code) as billing_code_raw,
    billing_code_type,
    description,
    payer_name,
    plan_name,
    rate_category,
    rate_amount,
    rate_unit,
    source_system,
    source_file_name,
    ingested_at,
    contracting_method
  from {{ ref('fct_standard_charges_semantic') }}
  where rate_category is not null
    and rate_amount is not null
    {% if var('execution_mode', 'bq') == 'local' %}
    and try_cast(rate_amount as double) is not null
    and try_cast(rate_amount as double) != 0
    {% else %}
    and safe_cast(rate_amount as numeric) is not null
    and safe_cast(rate_amount as numeric) != 0
    {% endif %}
),

with_keys as (
  select
    semantic_charge_sk,
    standard_charge_sk,
    hospital_id,
    hospital_name,
    billing_code_raw,
    {{ normalize_billing_code_for_storage('billing_code_raw', 'billing_code_type') }} as billing_code,
    billing_code_type,
    {{ is_valid_billing_code('billing_code_raw', 'billing_code_type') }} as billing_code_is_valid,
    {{ billing_code_issue_reason('billing_code_raw', 'billing_code_type') }} as billing_code_issue_reason,
    description,
    payer_name,
    plan_name,
    rate_category,
    rate_amount,
    rate_unit,
    source_system,
    source_file_name,
    ingested_at,
    contracting_method,
    lower(trim(cast(rate_category as string))) as rate_category_norm,
    trim(coalesce(cast(rate_unit as string), '')) as rate_unit_trim,
    concat(
      coalesce(cast(billing_code_type as string), ''),
      '|',
      lower(trim(coalesce(cast(rate_category as string), ''))),
      '|',
      coalesce(trim(coalesce(cast(rate_unit as string), '')), '')
    ) as comparability_key,
    case
      when lower(trim(coalesce(cast(rate_category as string), ''))) = 'other' then 'excluded_other'
      when lower(trim(coalesce(cast(rate_category as string), ''))) not in ({% for c in accepted_rate_categories %}'{{ c }}'{% if not loop.last %}, {% endif %}{% endfor %}) then 'excluded_unexpected_category'
      when lower(trim(coalesce(cast(rate_category as string), ''))) not in ({% for c in comparable_rate_categories %}'{{ c }}'{% if not loop.last %}, {% endif %}{% endfor %}) then 'excluded_other'
      else 'comparable'
    end as comparability_reason
  from semantic
)

select
  semantic_charge_sk,
  standard_charge_sk,
  hospital_id,
  hospital_name,
  billing_code_raw,
  billing_code,
  billing_code_type,
  billing_code_is_valid,
  billing_code_issue_reason,
  description,
  payer_name,
  plan_name,
  rate_category_norm as rate_category,
  rate_amount,
  rate_unit,
  comparability_key,
  comparability_reason = 'comparable' as is_comparable,
  comparability_reason,
  source_system,
  source_file_name,
  ingested_at,
  contracting_method
from with_keys
