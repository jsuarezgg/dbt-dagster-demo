{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--raw.ally_management_allies_br
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    slug AS ally_slug,
    name AS ally_name,
    vertical,
    brand,
    website,
    type,
    tags,
    categories,
    active AS active_ally,
    logo_url,
    economic_activity,
    logos,
    channel,
    ally_state,
    commercial_type,
    similars,
    acceptance_terms_conditions,
    additional_information,
    categories_v2,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw', 'ally_management_allies_br') }}
