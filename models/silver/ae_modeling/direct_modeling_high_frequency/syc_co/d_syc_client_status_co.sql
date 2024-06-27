{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--bronze.syc_client_status_co
SELECT
    -- DIRECT MODELING FIELDS
    calculation_date,
    client_id,
    delinquency_balance,
    full_payment,
    min_payment,
    payday,
    positive_balance,
    total_payment,
    total_payment_addi,
    total_payment_pa,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
from {{ ref('syc_client_status_co') }}