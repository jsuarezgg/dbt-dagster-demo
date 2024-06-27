{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


SELECT
    communication_id,
    communication_template,
    communication_version,
    communication_status,
    communication_source,
    communication_key,
    created_at,
    campaign_name,
    channel,
    recipient,
    message,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
        
-- DBT SOURCE REFERENCE
FROM {{ ref('communications_communication_br') }}

