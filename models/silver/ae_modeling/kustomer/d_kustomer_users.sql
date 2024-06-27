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
--bronze.kustomer_users
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    user_id,
    name,
    display_name,
    user_type,
    email,
    email_verified_at,
    first_email_verified_at,
    mobile,
    email_signature,
    password_allow_new,
    password_force_new,
    password_updated_at,
    role_groups,
    roles,
    created_at,
    updated_at,
    modified_at,
    deleted_at,
    org_id,
    created_by_id,
    modified_by_id,
    reference_date,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS data_platform_updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('kustomer_users') }}
