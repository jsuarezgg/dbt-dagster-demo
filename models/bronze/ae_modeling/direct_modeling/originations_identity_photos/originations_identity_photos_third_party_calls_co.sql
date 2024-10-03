{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.originations_identity_photos_third_party_calls_co
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    id AS call_id,
    application_id,
    client_id,
    provider,
    service_name,
    method_name,
    url,
    status_result,
    status_code,
    request_headers,
    request_raw_body,
    response_headers,
    response_raw_body,
    created_at,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw', 'originations_identity_photos_third_party_calls_co') }}
