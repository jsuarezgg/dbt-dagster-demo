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
    configuration_request_id,
    ally_slug,
    user_email,
    source,
    status,
    effective_date,
    report_term,
    payment_distance,
    reason,
    addi_pago_origination_mdf,
    data,
    configuration_request_created_at,
    configuration_request_updated_at,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM {{ ref('ally_management_ally_portal_payment_configuration_requests_co') }}
