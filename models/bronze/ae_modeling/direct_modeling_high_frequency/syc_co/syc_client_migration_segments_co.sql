{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.syc_client_migration_segments_co
SELECT
    -- DIRECT MODELING FIELDS
    client_id,
    segment AS lms_segment,
    created_at AS lms_migrated_at,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
from hive_metastore.raw.syc_client_migration_segments_co