{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--Volume: 16 by 2023-09-19
--raw_modeling.kus_statuses
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    statusId AS status_id,
    name AS name,
    statusType AS status_type,
    selectable AS selectable,
    system AS system,
    enabled AS enabled,
    routable AS routable,
    createdAt AS created_at,
    updatedAt AS updated_at,
    orgId AS org_id,
    referenceDate AS reference_date,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS data_platform_updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw_modeling', 'kus_statuses') }}