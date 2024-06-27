{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.ally_management_ally_promotions_co
WITH explode_product AS (
    SELECT
    id AS promotion_id,
    explode(from_json(data:productPolicies,'array<string>'))  AS discount_by_product
    FROM {{ source('raw', 'ally_management_ally_promotions_co') }}
)

SELECT 
    MD5(CONCAT(promotion_id,discount_by_product:producttype)) AS custom_product_policy_id,
    promotion_id,
    discount_by_product:producttype AS product_type,
    (discount_by_product:discountConfig:clientDiscountPercentage)::DOUBLE AS client_discount_percentage,
    (discount_by_product:discountConfig:prospectDiscountPercentage)::DOUBLE AS prospect_discount_percentage,
    (discount_by_product:discountConfig:allyDiscountPercentageAssumed)::DOUBLE AS ally_discount_percentage_assumed,
    (discount_by_product:discountConfig:addiDiscountPercentageAssumed)::DOUBLE AS addi_discount_percentage_assumed,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM explode_product