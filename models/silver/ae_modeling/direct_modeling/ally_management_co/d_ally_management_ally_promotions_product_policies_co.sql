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
    custom_product_policy_id,
    promotion_id,
    product_type,
    client_discount_percentage,
    prospect_discount_percentage,
    ally_discount_percentage_assumed,
    addi_discount_percentage_assumed,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM {{ ref('ally_management_ally_promotions_unnested_by_product_policy_co') }}
