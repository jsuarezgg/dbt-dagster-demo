{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


SELECT DISTINCT
    ally_slug,
    store_slug,
    data:['productPolicies'][0]['mdfFees']['originationMDF'] as origination_mdf_pago,
    data:['productPolicies'][1]['mdfFees']['originationMDF'] as origination_mdf_financia,
    --data:['productPolicies'][1]['prospectDiscount'] * 100 as prospect_discount_financia,
    --data:['productPolicies'][1]['clientDiscount'] * 100 as prospect_discount_financia,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
        
-- DBT SOURCE REFERENCE
FROM {{ ref('ally_management_ally_config_co') }} 
