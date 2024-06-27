{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.client_payments_client_payments_co
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    id,
    payment_method,
    amount,
    reference_code,
    client_id,
    cast(created_at AS TIMESTAMP),
    CAST(payment_date AS TIMESTAMP),
    origination_zone,
    CAST(annulled AS BOOLEAN),
    annulment_reason,
    payment_ownership,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw', 'client_payments_client_payments_co') }}
