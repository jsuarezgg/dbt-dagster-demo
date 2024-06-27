{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--Volume: 102 by 2023-09-19
--bronze.kustomer_queues
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    queue_id,
    name,
    display_name,
    priority,
    item_size,
    restrict_transfers_by_users,
    system,
    description,
    updated_at,
    created_at,
    modified_at,
    org_id,
    user_id,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS data_platform_updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('kustomer_queues') }}
