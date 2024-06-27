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
    id AS user_id,
    email,
    first_name,
    id_number,
    id_type,
    last_name,
    middle_name,
    second_last_name,
    ally_slug,
    acceptance_privacy_policy,
    current_store_slug,
    allies,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
        
-- DBT SOURCE REFERENCE
FROM {{ ref('ally_management_store_users_br') }} 

