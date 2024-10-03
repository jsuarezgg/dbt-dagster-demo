{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--addi_prod.raw.communications_marketing_preference_log_co
SELECT
    -- MANDATORY FIELDS
    id AS log_id,
    client_id,
    `type` AS preference_type,
    `source` AS preference_source,
    created_at::TIMESTAMP AS preference_created_at,
    -- DATA PLATFORM COLUMNS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
    -- CUSTOM ATTRIBUTES
    -- Fill with your custom attributes
-- DBT SOURCE REFERENCE
from {{ source('raw', 'communications_marketing_preference_log_co') }}
