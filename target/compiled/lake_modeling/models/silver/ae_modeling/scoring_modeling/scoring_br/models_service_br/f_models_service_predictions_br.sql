

--bronze.models_service_predictions_br
SELECT
    -- DIRECT MODELING FIELDS
    context_id,
    user_id,
    created_at,
    flow,
    output,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
from bronze.models_service_predictions_br