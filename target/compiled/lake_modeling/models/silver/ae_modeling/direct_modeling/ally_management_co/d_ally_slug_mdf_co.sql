


SELECT DISTINCT
    ally_slug,
    store_slug,
    data:['productPolicies'][0]['mdfFees']['originationMDF'] as origination_mdf_pago,
    data:['productPolicies'][1]['mdfFees']['originationMDF'] as origination_mdf_financia,
    --data:['productPolicies'][1]['prospectDiscount'] * 100 as prospect_discount_financia,
    --data:['productPolicies'][1]['clientDiscount'] * 100 as prospect_discount_financia,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
        
-- DBT SOURCE REFERENCE
FROM bronze.ally_management_ally_config_co