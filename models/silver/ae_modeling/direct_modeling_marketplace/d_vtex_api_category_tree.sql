{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--bronze.vtex_category_tree_api
SELECT
    -- MANDATORY FIELDS
    -- CUSTOM ATTRIBUTES
id AS category_id,
name AS category_name,
title AS category_title,
haschildren AS has_children,
children,
url,
NOW() AS ingested_at,
to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('bronze','vtex_category_tree_api') }}