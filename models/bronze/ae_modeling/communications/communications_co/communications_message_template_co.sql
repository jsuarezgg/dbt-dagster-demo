{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

-- raw.communications_message_template_co_5
SELECT
    id AS message_template_id,
    template_name,
    body,
    version,
    channel,
    category,
    created_at,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw', 'communications_message_template_co_5') }}
