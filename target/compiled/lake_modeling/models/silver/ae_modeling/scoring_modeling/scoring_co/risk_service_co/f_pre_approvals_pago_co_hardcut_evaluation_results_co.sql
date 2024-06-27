

--bronze.risk_service_decision_unit_logs_co
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    decision_id,
    execution_id,
    context_id,
    created_at,
    payload,
    decision_unit,
    flow,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
from bronze.risk_service_decision_unit_logs_co
where 1=1
and decision_unit = 'HardcutPoliciesBooleanDecisionUnit'
and flow = 'PREAPPROVALS_PAGO_CO'