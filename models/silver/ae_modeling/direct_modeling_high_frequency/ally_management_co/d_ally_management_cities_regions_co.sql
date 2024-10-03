{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

SELECT 
        city.city_code
        ,city.city_name
        ,city.region_code
        ,region.region_name
        ,NOW() AS ingested_at
        ,to_timestamp('{{ var("execution_date") }}') AS updated_at
        
-- DBT SOURCE REFERENCE
FROM {{ ref('ally_management_cities_co') }} AS city

LEFT JOIN

-- DBT SOURCE REFERENCE
 {{ ref('ally_management_regions_co') }} AS region

ON city.region_code=region.region_code
