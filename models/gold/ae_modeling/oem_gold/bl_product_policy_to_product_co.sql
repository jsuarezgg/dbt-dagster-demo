{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

-- Based on unique values from `type` in `silver.f_allies_product_policies_co`
-- Useful reference: --https://github.com/AdelanteFinancialHoldings/platform/blob/master/backend-jvm-legacy/channels/kernel/domain/src/main/java/com/addi/channels/kernel/domain/application/AbstractApplication.java#L289
-- Notion page: https://www.notion.so/addico/gold-bl_product_policy_to_product_co-a2376351d2da48b09e709dd5a31b814e?pvs=4

SELECT
    DISTINCT `type` AS ally_product_policy,
    CASE
        WHEN `type` = 'ADDI_FINANCIA' THEN 'FINANCIA_CO'
        WHEN `type` = 'ADDI_BNPN' THEN 'BNPN_CO'
        WHEN `type` = 'ADDI_PAGO' THEN 'PAGO_CO'
        ELSE '_MISSING_MAPPING_'
    END AS original_product,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM {{ ref('f_allies_product_policies_co') }}
WHERE `type` IS NOT NULL
