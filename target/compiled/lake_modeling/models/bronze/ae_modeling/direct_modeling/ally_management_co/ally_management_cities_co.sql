


--raw.ally_management_cities_co
SELECT 
        code as city_code,
        name as city_name,
        region_code,
    -- CUSTOM ATTRIBUTES
        NOW() AS ingested_at,
        to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
FROM raw.ally_management_cities_co