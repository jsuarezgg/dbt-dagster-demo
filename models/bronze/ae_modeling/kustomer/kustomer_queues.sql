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
--raw_modeling.kus_queues
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    queueId AS queue_id,
    name AS name,
    displayName AS display_name,
    priority AS priority,
    itemSize AS item_size,
    restrictTransfersByUsers AS restrict_transfers_by_users,
    system AS system,
    NULLIF(TRIM(description),'') AS description,
    updatedAt AS updated_at,
    createdAt AS created_at,
    modifiedAt AS modified_at,
    orgId AS org_id,
    userId AS user_id,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS data_platform_updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw_modeling', 'kus_queues') }}
WHERE queueId IS NOT NULL
