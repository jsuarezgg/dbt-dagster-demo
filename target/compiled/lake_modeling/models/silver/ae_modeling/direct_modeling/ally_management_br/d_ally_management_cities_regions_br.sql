

SELECT 
        city.city_code
        ,city.city_name
        ,city.region_code
        ,region.region_name
        ,NOW() AS ingested_at
        ,to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
FROM bronze.ally_management_cities_br AS city

LEFT JOIN

-- DBT SOURCE REFERENCE
 bronze.ally_management_regions_br AS region

ON city.region_code=region.region_code