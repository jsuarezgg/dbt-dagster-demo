{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


SELECT
    id,
    ally_slug,
    store_slug,
    data,
    start_date_validity,
    end_date_validity,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
        
-- DBT SOURCE REFERENCE
FROM {{ ref('ally_management_ally_config_co') }} 

