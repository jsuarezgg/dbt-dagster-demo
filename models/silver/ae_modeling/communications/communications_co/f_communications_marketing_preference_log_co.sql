{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

-- addi_prod.bronze.communications_marketing_preference_log_co
SELECT
    -- MANDATORY FIELDS
    log_id,
    client_id,
    preference_type,
    preference_source,
    preference_created_at,
    -- DATA PLATFORM COLUMNS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('communications_marketing_preference_log_co') }}
