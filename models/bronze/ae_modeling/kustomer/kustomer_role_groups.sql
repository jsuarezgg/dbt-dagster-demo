{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--Volume: 21 by 2023-09-19
--raw_modeling.kus_role_groups
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    roleGroupId AS role_group_id,
    name AS name,
    NULLIF(TRIM(description),'') AS description,
    display AS display,
    system AS system,
    roles AS roles,
    meta_web_access AS meta_web_access,
    meta_mapped_roles AS meta_mapped_roles,
    createdAt AS created_at,
    updatedAt AS updated_at,
    modifiedAt AS modified_at,
    rev AS rev,
    orgId AS org_id,
    createdById AS created_by_id,
    modifiedById AS modified_by_id,
    activeVersionId AS active_version_id,
    referenceDate AS reference_date,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS data_platform_updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw_modeling', 'kus_role_groups') }}