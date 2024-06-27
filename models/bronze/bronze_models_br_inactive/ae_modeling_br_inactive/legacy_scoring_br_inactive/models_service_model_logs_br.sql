{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.models_service_model_logs_br
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
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
from {{ source('raw', 'models_service_model_logs_br') }}
