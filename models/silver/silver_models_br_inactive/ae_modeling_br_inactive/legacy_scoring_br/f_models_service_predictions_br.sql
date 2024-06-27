{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

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
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
from {{ ref('models_service_predictions_br') }}
