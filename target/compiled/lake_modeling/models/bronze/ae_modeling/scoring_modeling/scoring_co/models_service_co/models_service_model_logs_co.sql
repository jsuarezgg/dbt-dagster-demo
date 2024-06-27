

--raw.models_service_model_logs_co
SELECT
    -- DIRECT MODELING FIELDS
	execution_id,
    context_id,
    created_at,
    model,
    input,
    output,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
from raw.models_service_model_logs_co