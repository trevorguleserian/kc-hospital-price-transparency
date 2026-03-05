{{
  config(
    materialized='table',
    schema='marts',
    tags=['local'],
  )
}}

/*
  One row per (payer_name_raw, plan_name_raw) from dim_payer with normalized and harmonized family fields.
  payer_family / plan_family come from editable seed maps; fallback to normalized name when no mapping.
*/

with raw as (
  select
    payer_name as payer_name_raw,
    plan_name as plan_name_raw
  from {{ ref('dim_payer') }}
),

with_norm as (
  select
    payer_name_raw,
    plan_name_raw,
    {{ normalize_text('payer_name_raw') }} as payer_name_norm,
    {{ normalize_text('plan_name_raw') }} as plan_name_norm
  from raw
),

with_payer_family as (
  select
    w.payer_name_raw,
    w.plan_name_raw,
    w.payer_name_norm,
    w.plan_name_norm,
    coalesce(p.payer_family, w.payer_name_norm) as payer_family
  from with_norm w
  left join {{ ref('seed_payer_map') }} p
    on w.payer_name_norm = p.payer_name_norm
),

with_plan_family as (
  select
    w.payer_name_raw,
    w.plan_name_raw,
    w.payer_name_norm,
    w.plan_name_norm,
    w.payer_family,
    coalesce(pl.plan_family, w.plan_name_norm) as plan_family
  from with_payer_family w
  left join {{ ref('seed_plan_map') }} pl
    on w.payer_family = pl.payer_family
    and w.plan_name_norm = pl.plan_name_norm
)

select
  payer_name_raw,
  plan_name_raw,
  payer_name_norm,
  plan_name_norm,
  payer_family,
  plan_family
from with_plan_family
