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
--bronze.kus_role_groups
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    role_group_id,
    name,
    description,
    display,
    system,
    roles,
    meta_web_access,
    meta_mapped_roles,
    created_at,
    updated_at,
    modified_at,
    rev,
    org_id,
    created_by_id,
    modified_by_id,
    active_version_id,
    reference_date,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS data_platform_updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('kustomer_role_groups') }}
