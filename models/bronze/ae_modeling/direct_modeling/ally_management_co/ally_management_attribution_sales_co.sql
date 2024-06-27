{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

-- raw.ally_management_attribution_sales_co
SELECT
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    id AS attribution_sale_id,
    loan_id,
    ally_slug,
    ally_seller_national_id_type,
    ally_seller_national_id_number,
    status,
    reason,
    created_at::timestamp AS attribution_sale_created_at,
    updated_at::timestamp AS attribution_sale_updated_at,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM {{ source('raw', 'ally_management_attribution_sales_co')}};
