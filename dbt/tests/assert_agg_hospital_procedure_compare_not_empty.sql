-- Fail if comparison mart is empty (guardrail: aggs must have rows for Streamlit).
-- Returns a row when the table is empty so the singular test fails.
select 1
where not exists (select 1 from {{ ref('agg_hospital_procedure_compare') }})
