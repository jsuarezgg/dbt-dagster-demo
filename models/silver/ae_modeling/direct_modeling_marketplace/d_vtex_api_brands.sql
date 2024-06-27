{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--bronze.vtex_brands_api
SELECT
    -- MANDATORY FIELDS
    -- CUSTOM ATTRIBUTES
id AS brand_id,
name AS brand_name,
isactive AS is_active,
NULLIF(TRIM(title),'') AS brand_title,
imageurl AS image_url,
metatagdescription AS metatag_description,
NOW() AS ingested_at,
to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('bronze','vtex_brands_api') }}