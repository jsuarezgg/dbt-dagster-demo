{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.ally_management_ally_activation_unnested_by_product_policies_co
WITH explode_items AS (
    SELECT
        -- MANDATORY FIELDS
        -- MAPPED FIELDS - DIRECT ATTRIBUTES
        slug AS ally_slug,
        EXPLODE(FROM_JSON(data:externalInfo.productPolicies, 'array<string>')) AS productPolicies,
        -- CUSTOM ATTRIBUTES
        NOW() AS ingested_at,
        to_timestamp('{{ var("execution_date") }}') AS updated_at
    -- DBT SOURCE REFERENCE
    FROM {{ source('raw', 'ally_management_ally_activation_co') }}
)
SELECT
    CONCAT('SLG_',ally_slug,'_PPPT_', productPolicies:productType) AS surrogate_key,
    ally_slug,
    productPolicies:productType AS external_info_product_type,
    productPolicies:mdfFees.fraudMDF AS external_info_fraud_mdf,
    productPolicies:mdfFees.originationMDF AS external_info_origination_mdf,
    productPolicies:mdfFees.cancellationMDF AS external_info_cancellation_mdf,
    productPolicies:reportConfig.reportTerm AS external_info_report_term,
    productPolicies:paymentConfig.paymentDistance AS external_info_payment_distance,
    ingested_at,
    updated_at
FROM explode_items