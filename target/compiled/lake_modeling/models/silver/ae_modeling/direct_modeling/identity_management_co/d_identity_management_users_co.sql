


SELECT
    id,
    email,
    idaas,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
        
-- DBT SOURCE REFERENCE
FROM bronze.identity_management_users_co