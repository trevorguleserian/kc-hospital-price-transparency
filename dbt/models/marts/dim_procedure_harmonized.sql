{{
  config(
    materialized='table',
    schema='marts',
    tags=['local'],
  )
}}

/*
  One row per (billing_code, billing_code_type) with canonical_description = most frequent
  non-null description across all hospitals, and description_variants_count = count of distinct descriptions.
  Input: fct_standard_charges_semantic for frequency; dim_procedure for full coverage of codes.
*/

with fact_counts as (
  select
    coalesce(billing_code, '') as billing_code,
    coalesce(cast(billing_code_type as string), 'UNKNOWN') as billing_code_type,
    trim(coalesce(cast(description as string), '')) as description_trim,
    count(*) as cnt
  from {{ ref('fct_standard_charges_semantic') }}
  where billing_code is not null
  group by 1, 2, 3
),

non_empty as (
  select *
  from fact_counts
  where description_trim != ''
),

ranked as (
  select
    billing_code,
    billing_code_type,
    description_trim,
    sum(cnt) over (partition by billing_code, billing_code_type) as total,
    row_number() over (partition by billing_code, billing_code_type order by cnt desc, description_trim) as rn
  from non_empty
),

canonical_from_fact as (
  select
    billing_code,
    billing_code_type,
    description_trim as canonical_description
  from ranked
  where rn = 1
),

variants as (
  select
    billing_code,
    billing_code_type,
    count(distinct description_trim) as description_variants_count
  from non_empty
  group by 1, 2
),

-- Full coverage: all (billing_code, billing_code_type) from dim_procedure
dim_keys as (
  select distinct
    coalesce(billing_code, '') as billing_code,
    coalesce(cast(billing_code_type as string), 'UNKNOWN') as billing_code_type
  from {{ ref('dim_procedure') }}
),

dim_fallback as (
  select
    coalesce(billing_code, '') as billing_code,
    coalesce(cast(billing_code_type as string), 'UNKNOWN') as billing_code_type,
    max(trim(coalesce(cast(description as string), ''))) as fallback_description
  from {{ ref('dim_procedure') }}
  group by 1, 2
),

with_canonical as (
  select
    k.billing_code,
    k.billing_code_type,
    coalesce(c.canonical_description, nullif(trim(df.fallback_description), '')) as canonical_description
  from dim_keys k
  left join canonical_from_fact c
    on k.billing_code = c.billing_code and k.billing_code_type = c.billing_code_type
  left join dim_fallback df on k.billing_code = df.billing_code and k.billing_code_type = df.billing_code_type
),

with_variants as (
  select
    w.billing_code,
    w.billing_code_type,
    w.canonical_description,
    coalesce(v.description_variants_count, 0) as description_variants_count
  from with_canonical w
  left join variants v
    on w.billing_code = v.billing_code and w.billing_code_type = v.billing_code_type
)

select
  billing_code,
  billing_code_type,
  coalesce(nullif(trim(canonical_description), ''), '(no description)') as canonical_description,
  description_variants_count
from with_variants
