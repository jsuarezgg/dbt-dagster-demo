{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--bronze.syc_refinancing_instructions_co
SELECT DISTINCT
    -- DIRECT MODELING FIELDS
    id,
    sri.client_id as client_id,
    loan_id,
    type,
    version,
    data,
    start_date,
    end_date,
    created_at,
    sri.updated_at as updated_at_source,
    annulled,
    annulment_reason,
    CASE WHEN scms.client_id is null then 'lms'
                                     else 'kordev'
                                     end as loan_tape_source,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
from {{ ref('syc_refinancing_instructions_co') }} sri
LEFT JOIN {{ ref('syc_client_migration_segments_co') }} scms
ON sri.client_id  = scms.client_id