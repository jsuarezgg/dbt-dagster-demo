{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--Volume: 1029 by 2023-09-19
--bronze.kustomer_tags
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    tag_id,
    name,
    color,
    created_at,
    updated_at,
    modified_at,
    deleted,
    deleted_at,
    org_id,
    created_by_id,
    modified_by_id,
    deleted_by_id,
    reference_date,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS data_platform_updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('kustomer_tags') }}
