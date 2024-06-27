
{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--raw.ally_management_regions_co
SELECT 
        code as region_code,
        name as region_name,
    -- CUSTOM ATTRIBUTES
        NOW() AS ingested_at,
        to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw', 'ally_management_regions_co') }}
