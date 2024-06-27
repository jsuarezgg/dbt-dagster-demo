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
    email,
    idaas,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
        
-- DBT SOURCE REFERENCE
FROM {{ ref('identity_management_users_co') }} 

