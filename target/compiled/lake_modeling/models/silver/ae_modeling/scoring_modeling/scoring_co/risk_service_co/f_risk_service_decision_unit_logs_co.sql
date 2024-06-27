

--bronze.risk_service_decision_unit_logs_co
SELECT
    -- DIRECT MODELING FIELDS
	decision_id,
	execution_id,
	context_id,
	created_at,
	payload,
	decision_unit,
	flow,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
from bronze.risk_service_decision_unit_logs_co