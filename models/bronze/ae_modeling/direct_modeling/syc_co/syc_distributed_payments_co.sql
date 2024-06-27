{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--raw.syc_distributed_payments_co
SELECT
 -- MAPPED FIELDS - DIRECT ATTRIBUTES
 id,
 client_id,
 payment_id,
 loan_id,
 CAST(amount AS DECIMAL(28,6)) as amount,
 -- CUSTOM ATTRIBUTES
NOW() AS ingested_at,
to_timestamp('{{ var("execution_date") }}') AS updated_at

-- DBT SOURCE REFERENCE
FROM {{ source('raw', 'syc_distributed_payments_co') }}
