{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.salesforce_record_type_full_refresh_overwrite
SELECT
    Id AS record_type_id,
    Name AS record_type_name,
    DeveloperName AS record_type_developer_name,
    IsActive AS record_type_is_active,
    (Description)::STRING AS record_type_description,
    SobjectType AS record_type_sobject_type,
    NamespacePrefix AS record_type_namespace_prefix,
    BusinessProcessId AS business_process_id,
	LastModifiedDate.member0 AS record_type_lastmoddate,
	SystemModstamp.member0 AS record_type_systemmodstamp,
    CreatedDate.member0 AS record_type_created_date,
    CreatedById AS record_type_created_by_id,
	-- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
    `_airbyte_emitted_at` AS airbyte_emitted_at,
    ingested_from_s3_at
    -- CUSTOM ATTRIBUTES
    -- Fill with your custom attributes
-- DBT SOURCE REFERENCE
FROM {{ source('raw', 'salesforce_record_type_full_refresh_overwrite') }}