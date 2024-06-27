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
--raw_modeling.kus_companies
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    companyId AS company_id,
    name AS name,
    createdAt AS created_at,
    updatedAt AS updated_at,
    modifiedAt AS modified_at,
    tags AS tags,
    domains AS domains,
    emails AS emails,
    phones AS phones,
    whatsapps AS whatsapps,
    socials AS socials,
    urls AS urls,
    locations AS locations,
    rev AS rev,
    roleGroupVersions AS role_group_versions,
    orgId AS org_id,
    createdById AS created_by_id,
    modifiedById AS modified_by_id,
    referenceDate AS reference_date,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS data_platform_updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw_modeling', 'kus_companies') }}
