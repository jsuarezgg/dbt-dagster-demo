


SELECT
    id,
    ally_slug,
    store_slug,
    data,
    start_date_validity,
    end_date_validity,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
        
-- DBT SOURCE REFERENCE
FROM bronze.ally_management_ally_config_co