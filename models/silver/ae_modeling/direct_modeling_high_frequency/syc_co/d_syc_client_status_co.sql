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
SELECT DISTINCT
    -- DIRECT MODELING FIELDS
    calculation_date,
    scs.client_id as client_id,
    delinquency_balance,
    full_payment,
    min_payment,
    payday,
    positive_balance,
    total_payment,
    total_payment_addi,
    total_payment_pa,
    CASE WHEN scms.client_id is null then 'lms'
                                     else 'kordev'
                                     end as loan_tape_source,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('syc_client_status_co') }} scs
LEFT JOIN {{ ref('syc_client_migration_segments_co') }} scms
ON scs.client_id  = scms.client_id