{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--raw.ally_management_ally_addi_marketplace_configurations_co
SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    id,
    ally_slug,
    source,
    data,
    created_at,
    start_date_validity,
    end_date_validity,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('raw', 'ally_management_ally_addi_marketplace_configurations_co') }}
