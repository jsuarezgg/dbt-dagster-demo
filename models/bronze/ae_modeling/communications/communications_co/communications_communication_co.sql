{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--raw.communications_communication_co_5
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
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
    category,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw', 'communications_communication_co_5') }}
QUALIFY ROW_NUMBER() OVER(PARTITION BY communication_id ORDER BY dms_commit DESC) = 1
