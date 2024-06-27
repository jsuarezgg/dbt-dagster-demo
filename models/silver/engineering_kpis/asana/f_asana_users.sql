{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
--bronze.asana_users
SELECT 
    gid,
    name, 
    email
FROM {{ source('bronze', 'asana_users') }}