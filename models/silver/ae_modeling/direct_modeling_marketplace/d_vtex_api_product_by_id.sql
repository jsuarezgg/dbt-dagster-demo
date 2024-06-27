{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--bronze.vtex_product_by_id_api
SELECT
    -- MANDATORY FIELDS
    -- CUSTOM ATTRIBUTES
id AS product_id,
name AS product_name,
departmentid AS department_id,
categoryid AS category_id,
brandid AS brand_id,
NULLIF(TRIM(refid),'') AS ref_id,
isvisible AS is_visible,
NULLIF(TRIM(description),'') AS description,
NULLIF(TRIM(descriptionshort),'') AS description_short,
releasedate::date AS release_date,
NULLIF(TRIM(keywords),'') AS keywords,
NULLIF(TRIM(title),'') AS product_title,
linkid AS link_id,
isactive AS is_active,
NULLIF(TRIM(metatagdescription),'') AS metatag_description,
showwithoutstock AS show_without_stock,
score,
NOW() AS ingested_at,
to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('bronze','vtex_product_by_id_api') }}