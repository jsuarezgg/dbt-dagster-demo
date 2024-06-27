{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.syc_client_payments_br
SELECT
    -- DIRECT MODELING FIELDS
    id as payment_id,
    payment_method,
    amount,
    reference_code,
    client_id,
    payment_date,
    origination_zone,
    payment_ownership,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
from {{ source('raw', 'syc_client_payments_br') }}