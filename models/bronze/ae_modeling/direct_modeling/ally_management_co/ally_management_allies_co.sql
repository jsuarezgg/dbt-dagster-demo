{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--raw.ally_management_allies_co
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
    cast(active as BOOLEAN) AS active_ally,
    logo_url,
    economic_activity,
    logos,
    description,
    channel,
    ally_state,
    commercial_type,
    similars,
    cast(acceptance_terms_conditions as BOOLEAN) as acceptance_terms_conditions,
    additional_information,
    categories_v2,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw', 'ally_management_allies_co') }}
