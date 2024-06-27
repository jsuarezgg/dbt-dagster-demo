


--raw.ally_management_store_users_br
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    id,
    email,
    first_name,
    id_number,
    id_type,
    last_name,
    middle_name,
    second_last_name,
    ally_slug,
    acceptance_privacy_policy,
    current_store_slug,
    allies,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
FROM raw.ally_management_store_users_br