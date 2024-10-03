{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

-- bronze.communications_clients_co
SELECT
    client_id,
    id_type,
    id_number,
    phone_number,
    email,
    data,
    preferences,
    device_tokens,
    created_at,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('communications_clients_co') }}
