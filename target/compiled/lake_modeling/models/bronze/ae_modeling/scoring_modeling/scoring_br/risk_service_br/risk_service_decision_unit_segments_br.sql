

--raw.risk_service_decision_unit_segments_br
SELECT
    -- DIRECT MODELING FIELDS
	id,
    valid_from,
    valid_to,
    type,
    value,
    flow,
    module,
    decision_unit,
    configs,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
from raw.risk_service_decision_unit_segments_br