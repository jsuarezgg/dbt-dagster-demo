{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--raw.identity_management_identifications_co
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    user_id AS client_id_or_user_id,
    national_id_type AS id_type,
    national_id_number AS id_number,
    email_address,
    phone_number,
    verifications,
    created_at::TIMESTAMP AS client_or_user_created_at,
    updated_at::TIMESTAMP AS client_or_user_updated_at,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw', 'identity_management_identifications_co') }}
