{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--Volume: 1322 by 2023-09-19
--raw_modeling.kus_users
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    userId AS user_id,
    NULLIF(TRIM(name),'') AS name,
    NULLIF(TRIM(displayName),'') AS display_name,
    userType AS user_type,
    email AS email,
    emailVerifiedAt AS email_verified_at,
    firstEmailVerifiedAt AS first_email_verified_at,
    mobile AS mobile,
    emailSignature AS email_signature,
    password_allowNew AS password_allow_new,
    password_forceNew AS password_force_new,
    password_updatedAt AS password_updated_at,
    roleGroups AS role_groups,
    roles AS roles,
    createdAt AS created_at,
    updatedAt AS updated_at,
    modifiedAt AS modified_at,
    deletedAt AS deleted_at,
    orgId AS org_id,
    createdById AS created_by_id,
    modifiedById AS modified_by_id,
    referenceDate AS reference_date,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS data_platform_updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw_modeling', 'kus_users') }}