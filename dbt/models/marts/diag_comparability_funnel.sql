{{
  config(
    materialized='view',
    schema='marts',
    tags=['diagnostic'],
  )
}}

/*
  Diagnostic: comparability funnel counts and breakdowns to debug empty agg tables.
  Run: dbt run --select diag_comparability_funnel, then query this view.
  Metrics: total_semantic_rows, accepted_category_rows, comparable_true_rows,
  comparable_true_non_other_rows, comparable_false_rows; breakdowns by comparability_reason.
*/
with semantic as (
  select * from {{ ref('fct_standard_charges_semantic') }}
),
comparable as (
  select * from {{ ref('fct_rates_comparable') }}
),
harmonized as (
  select * from {{ ref('fct_rates_comparable_harmonized') }}
),

funnel_counts as (
  select
    (select count(*) from semantic) as total_semantic_rows,
    (select count(*) from comparable where lower(trim(cast(rate_category as string))) in ('negotiated', 'gross', 'cash', 'min', 'max', 'percentage', 'other')) as accepted_category_rows,
    (select count(*) from harmonized where coalesce(is_comparable, false) = true) as comparable_true_rows,
    (select count(*) from harmonized where coalesce(is_comparable, false) = true and lower(trim(cast(rate_category as string))) != 'other') as comparable_true_non_other_rows,
    (select count(*) from comparable where coalesce(is_comparable, false) = false) as comparable_false_rows
),
funnel_row as (
  select
    total_semantic_rows,
    accepted_category_rows,
    comparable_true_rows,
    comparable_true_non_other_rows,
    comparable_false_rows
  from funnel_counts
),

by_rate_category as (
  select
    'breakdown' as section,
    'rate_category' as metric_name,
    cast(lower(trim(cast(rate_category as string))) as string) as breakdown_key,
    count(*) as n
  from comparable
  group by 3
),
by_is_comparable as (
  select
    'breakdown',
    'is_comparable',
    cast(is_comparable as string),
    count(*)
  from comparable
  group by 2, 3
),
by_comparability_reason as (
  select
    'breakdown',
    'comparability_reason',
    cast(comparability_reason as string),
    count(*)
  from comparable
  group by 2, 3
),
by_rate_unit_blank as (
  select
    'breakdown',
    'rate_unit_blank',
    case when trim(coalesce(cast(rate_unit as string), '')) = '' then 'blank' else 'nonblank' end,
    count(*)
  from comparable
  group by 2, 3
)

select 'funnel' as section, 'total_semantic_rows' as metric_name, cast(null as string) as breakdown_key, total_semantic_rows as n from funnel_row
union all select 'funnel', 'accepted_category_rows', cast(null as string), accepted_category_rows from funnel_row
union all select 'funnel', 'comparable_true_rows', cast(null as string), comparable_true_rows from funnel_row
union all select 'funnel', 'comparable_true_non_other_rows', cast(null as string), comparable_true_non_other_rows from funnel_row
union all select 'funnel', 'comparable_false_rows', cast(null as string), comparable_false_rows from funnel_row
union all select * from by_rate_category
union all select * from by_is_comparable
union all select * from by_comparability_reason
union all select * from by_rate_unit_blank
order by section, metric_name, n desc