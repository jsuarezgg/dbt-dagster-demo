{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--bronze.syc_client_refunds
SELECT DISTINCT
    -- DIRECT MODELING FIELDS
    id,
    scr.client_id as client_id,
    amount,
    paid_at,
    CASE WHEN scms.client_id is null then 'lms'
                                     else 'kordev'
                                     end as loan_tape_source,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('syc_client_refunds_co') }} scr
LEFT JOIN {{ ref('syc_client_migration_segments_co') }} scms
ON scr.client_id  = scms.client_id