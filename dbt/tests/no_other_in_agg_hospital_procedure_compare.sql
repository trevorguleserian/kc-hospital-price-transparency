-- Fail if rate_category = 'other' (or any casing/spacing variant) appears in comparison mart.
-- Normalize with lower(trim()) so variants like "Other" or " other " are caught.
select *
from {{ ref('agg_hospital_procedure_compare') }}
where lower(trim(cast(rate_category as string))) = 'other'
limit 10
