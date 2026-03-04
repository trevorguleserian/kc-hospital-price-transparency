{{ config(severity = 'warn') }}
-- Warn when a high proportion of dim_procedure rows have billing_code_type = 'UNKNOWN'.
-- Heuristics in dim_procedure should reduce UNKNOWN; this test warns if most rows stay UNKNOWN.
with cts as (
  select count(*) as total,
         sum(case when billing_code_type = 'UNKNOWN' then 1 else 0 end) as unknown_count
  from {{ ref('dim_procedure') }}
)
select 1 as warn_high_unknown_pct
from cts
where total > 0 and (cast(unknown_count as float) / total) >= 0.95
