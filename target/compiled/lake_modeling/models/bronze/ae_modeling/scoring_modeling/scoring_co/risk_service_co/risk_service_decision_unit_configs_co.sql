

--raw.risk_service_decision_unit_configs_co
SELECT
    -- DIRECT MODELING FIELDS
	id,
    name,
    valid_from,
    valid_to,
    module,
    decision_unit,
    configuration,
    flow,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
from raw.risk_service_decision_unit_configs_co