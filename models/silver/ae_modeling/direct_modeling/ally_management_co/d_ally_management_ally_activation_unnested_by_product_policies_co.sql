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
    surrogate_key,
    ally_slug,
    external_info_product_type,
    external_info_fraud_mdf,
    external_info_origination_mdf,
    external_info_cancellation_mdf,
    external_info_report_term,
    external_info_payment_distance,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM {{ ref('ally_management_ally_activation_unnested_by_product_policies_co') }}
