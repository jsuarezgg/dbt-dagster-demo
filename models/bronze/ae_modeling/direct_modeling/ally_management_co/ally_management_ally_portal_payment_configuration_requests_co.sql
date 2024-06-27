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
        id AS configuration_request_id,
        data:['allySlug'] AS ally_slug,
        data:['user']['email'] AS user_email,
        data:['source'] AS source,
        data:['status'] AS status,
        data:['effectiveDate'] AS effective_date,
        data:['paymentConfigurationType']['reportTerm'] AS report_term,
        data:['paymentConfigurationType']['paymentDistance'] AS payment_distance,
        data:['paymentConfigurationType']['reason'] AS reason,
        explode_outer(from_json(data:['paymentConfigurationType']['originationProducts'], 'array<string>')) AS origination_product_details,
        data,
        created_at::timestamp AS configuration_request_created_at,
        updated_at::timestamp AS configuration_request_updated_at
    FROM {{ source('raw', 'ally_management_ally_portal_payment_configuration_requests_co') }}

)

SELECT
    configuration_request_id,
    ally_slug,
    user_email,
    source,
    status,
    effective_date,
    report_term,
    payment_distance,
    reason,
    CASE WHEN
        origination_product_details:['productType'] = 'ADDI_PAGO' THEN origination_product_details:['originationMdf']
    END AS addi_pago_origination_mdf,
    data,
    configuration_request_created_at,
    configuration_request_updated_at,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM select_explode
WHERE (origination_product_details:['productType'] = 'ADDI_PAGO' OR origination_product_details:['productType'] IS NULL)
