{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--Volume: 40159 by 2023-09-20
--raw_modeling.kus_teams_history
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    teamId AS team_id,
    referenceDate AS reference_date,
    MD5(CONCAT(teamId,(referenceDate::STRING))) AS surrogate_key,
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
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS data_platform_updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw_modeling', 'kus_teams_history') }}