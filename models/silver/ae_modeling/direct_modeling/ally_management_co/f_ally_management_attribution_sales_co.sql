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
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    attribution_sale_id,
    loan_id,
    ally_slug,
    ally_seller_national_id_type,
    ally_seller_national_id_number,
    status,
    reason,
    attribution_sale_created_at,
    attribution_sale_updated_at,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM {{ ref('ally_management_attribution_sales_co')}};
