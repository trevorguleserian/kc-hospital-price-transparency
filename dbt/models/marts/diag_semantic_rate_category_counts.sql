{{
  config(
    materialized='view',
    schema='marts',
    tags=['diagnostic'],
  )
}}

/*
  Diagnostic: counts by source_system, rate_category, and raw rate_type from base.
  Shows what rate_type values exist upstream and what rate_category they map to (same logic as semantic primary_rows).
  Run: dbt run --select diag_semantic_rate_category_counts, then query this view.
*/
with base as (
  select
    source_system,
    rate_type,
    rate_amount,
    gross_charge,
    discounted_cash,
    minimum,
    maximum
  from {{ ref('int_standard_charges_base') }}
  where billing_code is not null
  and rate_amount is not null
  and safe_cast(rate_amount as numeric) is not null
  and safe_cast(rate_amount as numeric) != 0
),

with_derived as (
  select
    source_system,
    rate_type as raw_rate_type,
    lower(trim(coalesce(cast(rate_type as string), ''))) as rate_type_norm,
    coalesce(nullif(trim(
      case
        when lower(trim(coalesce(cast(rate_type as string), ''))) in ('negotiated_rate', 'negotiated_dollar', 'negotiated', 'estimated_amount', 'estimated', 'self_pay', 'self_pay_rate', 'self-pay') then 'negotiated'
        when lower(trim(coalesce(cast(rate_type as string), ''))) in ('negotiated_percentage', 'percentage') then 'percentage'
        when lower(trim(coalesce(cast(rate_type as string), ''))) in ('gross_charge', 'gross', 'standard_charge_gross', 'standard_charge', 'charge') then 'gross'
        when lower(trim(coalesce(cast(rate_type as string), ''))) in ('discounted_cash', 'discounted_cash_price', 'cash_price', 'cash') then 'cash'
        when lower(trim(coalesce(cast(rate_type as string), ''))) in ('min', 'minimum') then 'min'
        when lower(trim(coalesce(cast(rate_type as string), ''))) in ('max', 'maximum') then 'max'
        when lower(trim(coalesce(cast(rate_type as string), ''))) = '' and gross_charge is not null and safe_cast(rate_amount as numeric) = safe_cast(gross_charge as numeric) then 'gross'
        when lower(trim(coalesce(cast(rate_type as string), ''))) = '' and discounted_cash is not null and safe_cast(rate_amount as numeric) = safe_cast(discounted_cash as numeric) then 'cash'
        when lower(trim(coalesce(cast(rate_type as string), ''))) = '' and minimum is not null and safe_cast(rate_amount as numeric) = safe_cast(minimum as numeric) then 'min'
        when lower(trim(coalesce(cast(rate_type as string), ''))) = '' and maximum is not null and safe_cast(rate_amount as numeric) = safe_cast(maximum as numeric) then 'max'
        when lower(trim(coalesce(cast(rate_type as string), ''))) = '' then 'negotiated'
        else 'other'
      end
    ), ''), 'other') as derived_rate_category
  from base
  where rate_amount is not null and safe_cast(rate_amount as numeric) is not null and safe_cast(rate_amount as numeric) != 0
)

select
  source_system,
  raw_rate_type,
  rate_type_norm,
  derived_rate_category,
  count(*) as row_count
from with_derived
group by 1, 2, 3, 4
order by source_system, row_count desc, raw_rate_type
