{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--bronze.schema_stats_calculator_exclusions
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    uuid,
    runtime,
    country,
    delta_table,
    event_full_name,
    element,
    idx,
    drop_reason,
    type,
    subtype,
    path_dot_support,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('bronze', 'schema_stats_calculator_exclusions') }}
