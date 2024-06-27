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
        ur.user_id,
        ur.role_id,
        collect_list(rp.permission_id) AS permissions,
        NOW() AS ingested_at,
        to_timestamp('{{ var("execution_date") }}') AS updated_at
        
-- DBT SOURCE REFERENCE
FROM {{ ref('identity_management_users_roles_co') }} AS ur

LEFT JOIN

-- DBT SOURCE REFERENCE
 {{ ref('identity_management_roles_permissions_co') }} AS rp

ON ur.role_id=rp.role_id
GROUP BY ur.user_id,
         ur.role_id