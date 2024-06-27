{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH select_explode AS (

    SELECT
        id AS configuration_type_id,
        data:['allySlug'] AS ally_slug,
        data:['reason'] AS reason,
        data:['reportTerm'] AS report_term,
        data:['paymentDistance'] AS payment_distance,
        explode_outer(from_json(data:['originationProducts'], 'array<string>')) AS origination_product_details,
        data,
        created_at::timestamp AS configuration_type_created_at
    FROM {{ source('raw', 'ally_management_ally_portal_payment_configuration_types_co') }}

)

SELECT
    configuration_type_id,
    ally_slug,
    reason,
    report_term,
    payment_distance,
    CASE WHEN
        origination_product_details:['productType'] = 'ADDI_PAGO' THEN origination_product_details:['originationMdf']
    END AS addi_pago_origination_mdf,
    data,
    configuration_type_created_at,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM select_explode
WHERE (origination_product_details:['productType'] = 'ADDI_PAGO' OR origination_product_details:['productType'] IS NULL)
