


--raw.ally_management_regions_br
SELECT 
        code as region_code,
        name as region_name,
    -- CUSTOM ATTRIBUTES
        NOW() AS ingested_at,
        to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
FROM raw.ally_management_regions_br