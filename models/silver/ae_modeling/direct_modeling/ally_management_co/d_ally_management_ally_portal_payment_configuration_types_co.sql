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
    configuration_type_id,
    ally_slug,
    reason,
    report_term,
    payment_distance,
    addi_pago_origination_mdf,
    data,
    configuration_type_created_at,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM {{ ref('ally_management_ally_portal_payment_configuration_types_co') }}
