{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.syc_client_refunds
SELECT
    -- DIRECT MODELING FIELDS
    id,
    client_id,
    amount,
    paid_at,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
from {{ source('raw', 'syc_client_refunds_co') }}