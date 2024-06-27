{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--bronze.vtex_category_by_id_api
SELECT
    -- MANDATORY FIELDS
    -- CUSTOM ATTRIBUTES
id AS category_id,
name AS category_name,
title AS category_title,
haschildren AS has_children,
fathercategoryid AS father_category_id,
description,
keywords,
isactive AS is_active,
showinstorefront AS show_in_store_front,
showbrandfilter AS show_brand_filter,
activestorefrontlink AS active_store_front_link,
globalcategoryid AS global_category_id,
score,
linkid AS link_id,
NOW() AS ingested_at,
to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('bronze','vtex_category_by_id_api') }}