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
    id AS payment_id,
    payment_method,
    amount,
    reference_code,
    client_id,
    created_at,
    payment_date,
    origination_zone,
    annulled,
    annulment_reason,
    payment_ownership,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('client_payments_client_payments_co') }}
