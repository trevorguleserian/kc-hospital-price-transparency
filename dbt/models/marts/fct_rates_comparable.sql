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
  rate_category is normalized (LOWER(TRIM)). is_comparable = TRUE only for categories
  used in hospital/payer comparisons (negotiated, gross, cash, min, max, percentage).
  rate_category = 'other' is kept for diagnostics but is_comparable = FALSE.
  comparability_reason explains why a row is or is not comparable.
*/

{# Categories allowed in the table (accepted_values); includes other. #}
{% set accepted_rate_categories = var('accepted_rate_categories', ['negotiated', 'gross', 'cash', 'min', 'max', 'percentage', 'other']) %}
{# Categories that are comparable (is_comparable = TRUE); excludes other. #}
{% set comparable_rate_categories = var('comparable_rate_categories', ['negotiated', 'gross', 'cash', 'min', 'max', 'percentage']) %}
{% set unitless_rate_categories = var('unitless_rate_categories', ['percentage']) %}
{% set unit_optional_rate_categories = var('unit_optional_rate_categories', ['negotiated', 'gross', 'cash', 'min', 'max', 'percentage']) %}

with semantic as (
  select
    semantic_charge_sk,
    standard_charge_sk,
    hospital_id,
    hospital_name,
    billing_code,
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
    *,
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
      when lower(trim(coalesce(cast(rate_category as string), ''))) not in ({% for c in accepted_rate_categories %}'{{ c }}'{% if not loop.last %}, {% endif %}{% endfor %}) then 'category_not_allowed'
      when lower(trim(coalesce(cast(rate_category as string), ''))) = 'other' then 'category_not_allowed'
      when lower(trim(coalesce(cast(rate_category as string), ''))) not in ({% for c in comparable_rate_categories %}'{{ c }}'{% if not loop.last %}, {% endif %}{% endfor %}) then 'category_not_allowed'
      when trim(coalesce(cast(rate_unit as string), '')) != '' then 'ALLOWLIST_AND_HAS_UNIT'
      when lower(trim(coalesce(cast(rate_category as string), ''))) in ({% for c in unitless_rate_categories %}'{{ c }}'{% if not loop.last %}, {% endif %}{% endfor %}) then 'ALLOWLIST_UNIT_MISSING_BUT_ALLOWED'
      when lower(trim(coalesce(cast(rate_category as string), ''))) in ({% for c in unit_optional_rate_categories %}'{{ c }}'{% if not loop.last %}, {% endif %}{% endfor %}) then 'ALLOWLIST_UNIT_MISSING_BUT_ALLOWED'
      else 'missing_rate_unit'
    end as comparability_reason
  from semantic
)

select
  semantic_charge_sk,
  standard_charge_sk,
  hospital_id,
  hospital_name,
  billing_code,
  billing_code_type,
  description,
  payer_name,
  plan_name,
  rate_category_norm as rate_category,
  rate_amount,
  rate_unit,
  comparability_key,
  comparability_reason in ('ALLOWLIST_AND_HAS_UNIT', 'ALLOWLIST_UNIT_MISSING_BUT_ALLOWED') as is_comparable,
  comparability_reason,
  source_system,
  source_file_name,
  ingested_at,
  contracting_method
from with_keys
