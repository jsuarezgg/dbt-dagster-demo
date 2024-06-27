{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--bronze.syc_client_payments_br
SELECT
    -- DIRECT MODELING FIELDS
    payment_id,
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
from {{ ref('syc_client_payments_br') }}