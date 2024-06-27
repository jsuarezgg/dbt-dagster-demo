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
--bronze.kustomer_teams_history
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    team_id,
    reference_date,
    surrogate_key,
    icon,
    name,
    display_name,
    created_at,
    updated_at,
    modified_at,
    deleted,
    deleted_at,
    members_users_id_array,
    role_groups,
    org_id,
    created_by_id,
    modified_by_id,
    deleted_by_id,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS data_platform_updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('kustomer_teams_history') }}
