

--bronze.models_service_request_logs_br
SELECT
    -- DIRECT MODELING FIELDS
    execution_id,
    context_id,
    user_id,
    created_at,
    url,
    request,
    response,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
from bronze.models_service_request_logs_br