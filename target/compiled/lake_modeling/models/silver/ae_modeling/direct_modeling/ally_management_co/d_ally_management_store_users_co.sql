


SELECT
    id AS user_id,
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
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
        
-- DBT SOURCE REFERENCE
FROM bronze.ally_management_store_users_co