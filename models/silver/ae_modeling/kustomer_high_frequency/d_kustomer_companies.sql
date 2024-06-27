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
--bronze.kustomer_companies
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    company_id,
    name,
    created_at,
    updated_at,
    modified_at,
    tags,
    domains,
    emails,
    phones,
    whatsapps,
    socials,
    urls,
    locations,
    rev,
    role_group_versions,
    org_id,
    created_by_id,
    modified_by_id,
    reference_date,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS data_platform_updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('kustomer_companies') }}
