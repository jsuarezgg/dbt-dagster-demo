{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--bronze.syc_client_payments_co
SELECT DISTINCT
    -- DIRECT MODELING FIELDS
    payment_id,
    payment_method,
    amount,
    reference_code,
    scp.client_id as client_id,
    payment_date,
    origination_zone,
    payment_ownership,
    CASE WHEN scms.client_id is null then 'lms'
                                     else 'kordev'
                                     end as loan_tape_source,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
from {{ ref('syc_client_payments_co') }} scp
LEFT JOIN {{ ref('syc_client_migration_segments_co') }} scms
ON scp.client_id  = scms.client_id