{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--bronze.syc_distributed_payments_co
SELECT
 -- MAPPED FIELDS - DIRECT ATTRIBUTES
 id,
 client_id,
 payment_id,
 loan_id,
 amount,
 -- CUSTOM ATTRIBUTES
NOW() AS ingested_at,
to_timestamp('{{ var("execution_date") }}') AS updated_at

-- DBT SOURCE REFERENCE
FROM {{ ref('syc_distributed_payments_co') }}
