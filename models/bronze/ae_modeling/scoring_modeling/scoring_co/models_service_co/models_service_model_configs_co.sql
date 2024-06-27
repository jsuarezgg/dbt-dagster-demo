{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.models_service_model_configs_co
SELECT
    -- DIRECT MODELING FIELDS
	id,
    type,
    flow,
    name,
    internal_name,
    created_at,
    is_active,
    is_shadow,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
from {{ source('raw', 'models_service_model_configs_co') }}
