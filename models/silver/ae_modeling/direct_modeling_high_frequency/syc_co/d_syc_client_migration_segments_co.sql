{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--bronze.syc_client_migration_segments_co
SELECT
    -- DIRECT MODELING FIELDS
    client_id,
    lms_segment,
    CASE WHEN CAST(lms_migrated_at as DATE) >= '2024-09-30' THEN from_utc_timestamp(lms_migrated_at, 'America/Bogota')
         ELSE lms_migrated_at END as lms_migrated_at,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
from {{ ref('syc_client_migration_segments_co') }}