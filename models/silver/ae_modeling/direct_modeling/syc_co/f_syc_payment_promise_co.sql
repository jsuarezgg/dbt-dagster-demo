{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}



--bronze.syc_payment_promise_co
SELECT
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
id,
client_id,
state,
expected_amount,
start_date,
end_date,
conditions,
 -- CUSTOM ATTRIBUTES
NOW() AS ingested_at,
to_timestamp('{{ var("execution_date") }}') AS updated_at

-- DBT SOURCE REFERENCE
FROM {{ ref('syc_payment_promise_co') }}

