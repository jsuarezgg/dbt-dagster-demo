


--raw.identity_management_users_roles_co
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    user_id,
    role_id,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
FROM raw.identity_management_users_roles_co