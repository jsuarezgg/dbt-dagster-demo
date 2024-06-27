

--raw.offers_service_offers_logs_br
SELECT
    -- DIRECT MODELING FIELDS
	id as decision_id,
	context_id,
    user_id,
	created_at,
	flow,
	payload,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
from raw.offers_service_offers_logs_br