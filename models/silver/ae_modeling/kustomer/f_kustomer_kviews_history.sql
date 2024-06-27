{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--Volume: 33607 by 2023-09-20
--bronze.kustomer_kviews_history
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    kview_id,
    reference_date,
    surrogate_key,
    type,
    resource,
    display_name,
    enabled,
    created_at,
    updated_at,
    org_id,
    custom_attribute_name_array,
    custom_attribute_display_name_array,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS data_platform_updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('kustomer_kviews_history') }}
