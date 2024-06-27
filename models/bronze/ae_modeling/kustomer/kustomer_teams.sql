{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--Volume: 91 by 2023-09-19
--raw_modeling.kus_teams
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    teamId AS team_id,
    icon AS icon,
    NULLIF(TRIM(name),'') AS name,
    NULLIF(TRIM(displayName),'') AS display_name,
    createdAt AS created_at,
    updatedAt AS updated_at,
    modifiedAt AS modified_at,
    deleted AS deleted,
    deletedAt AS deleted_at,
    members AS members_users_id_array,
    roleGroups AS role_groups,
    orgId AS org_id,
    createdById AS created_by_id,
    modifiedById AS modified_by_id,
    deletedById AS deleted_by_id,
    referenceDate AS reference_date,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS data_platform_updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw_modeling', 'kus_teams') }}