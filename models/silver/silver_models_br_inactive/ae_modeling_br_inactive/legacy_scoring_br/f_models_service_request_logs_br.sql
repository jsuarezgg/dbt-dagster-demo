{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

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
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
from {{ ref('models_service_request_logs_br') }}
