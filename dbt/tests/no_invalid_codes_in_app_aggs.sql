-- Fail if any row in app-facing aggs has an invalid billing code (format violation).
-- These aggs are built with billing_code_is_valid = true; this test guards against regressions.
select billing_code, billing_code_type, 'agg_hospital_procedure_compare' as agg_name
from {{ ref('agg_hospital_procedure_compare') }}
where {{ billing_code_issue_reason('billing_code', 'billing_code_type') }} is not null
limit 10
union all
select billing_code, billing_code_type, 'agg_payer_plan_compare' as agg_name
from {{ ref('agg_payer_plan_compare') }}
where {{ billing_code_issue_reason('billing_code', 'billing_code_type') }} is not null
limit 10
