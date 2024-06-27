{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.client_payments_client_payments_br
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    id,
    payment_method,
    amount,
    reference_code,
    client_id,
    CAST(created_at AS TIMESTAMP) as created_at,
    CAST(payment_date AS TIMESTAMP) as payment_date,
    origination_zone,
    CAST(annulled as BOOLEAN) as annulled,
    annulment_reason,
    payment_ownership,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw', 'client_payments_client_payments_br') }}
