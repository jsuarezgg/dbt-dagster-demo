

--raw.risk_service_decision_unit_logs_br
SELECT
    -- DIRECT MODELING FIELDS
	id as decision_id,
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
from raw.risk_service_decision_unit_logs_br