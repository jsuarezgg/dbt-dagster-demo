{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--Volume: 1029 by 2023-09-19
--raw_modeling.kus_tags
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    tagId AS tag_id,
    NULLIF(TRIM(name),'') AS name,
    color AS color,
    createdAt AS created_at,
    updatedAt AS updated_at,
    modifiedAt AS modified_at,
    deleted AS deleted,
    deletedAt AS deleted_at,
    orgId AS org_id,
    createdById AS created_by_id,
    modifiedById AS modified_by_id,
    deletedById AS deleted_by_id,
    referenceDate AS reference_date,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS data_platform_updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw_modeling', 'kus_tags') }}