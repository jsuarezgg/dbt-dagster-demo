{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.kyc_blacklisted_emails_co
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    email,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw', 'kyc_blacklisted_emails_co') }}
