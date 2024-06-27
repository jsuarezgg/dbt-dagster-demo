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
    sub.category_slug,
    sub.sub_category_slug,
    cat.category_name,
    sub.sub_category_name,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('ally_management_ally_sub_categories_co') }} sub
LEFT JOIN {{ ref('ally_management_ally_categories_co') }} cat ON sub.category_slug=cat.category_slug