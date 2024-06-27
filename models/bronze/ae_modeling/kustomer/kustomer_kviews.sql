{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--Volume: 14841 by 2023-09-19
--raw_modeling.kus_kviews
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    kviewId AS kview_id,
    type AS type,
    resource AS resource,
    displayName AS display_name,
    enabled AS enabled,
    createdAt AS created_at,
    updatedAt AS updated_at,
    orgId AS org_id,
    customAttributeName AS custom_attribute_name_array,
    customAttributeDisplayName AS custom_attribute_display_name_array,
    referenceDate AS reference_date,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS data_platform_updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw_modeling', 'kus_kviews') }}
