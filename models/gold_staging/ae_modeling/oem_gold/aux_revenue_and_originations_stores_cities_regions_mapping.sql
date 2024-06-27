{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
WITH
ally_management_stores (
    SELECT "CO" AS country_code, store_slug, store_city_code FROM silver.d_ally_management_stores_allies_co WHERE store_slug IS NOT NULL
    UNION ALL
    SELECT "BR" AS country_code, store_slug, store_city_code FROM silver.d_ally_management_stores_allies_br WHERE store_slug IS NOT NULL
)
,
ally_management_cities_regions (
    SELECT  "CO" as country_code, city_code, city_name, region_name FROM silver.d_ally_management_cities_regions_co
    UNION ALL
    SELECT  "BR" as country_code, city_code, city_name, region_name FROM silver.d_ally_management_cities_regions_br
)

SELECT
    s.country_code,
    s.store_slug,
    s.store_city_code,
    cr.city_name,
    cr.region_name,
    -- DATA PLATFORM COLUMNS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM      ally_management_stores         AS s
LEFT JOIN ally_management_cities_regions AS cr ON s.store_city_code = cr.city_code AND s.country_code = cr.country_code