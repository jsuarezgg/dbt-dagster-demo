

SELECT 
        ur.user_id,
        ur.role_id,
        collect_list(rp.permission_id) AS permissions,
        NOW() AS ingested_at,
        to_timestamp('2022-01-01') AS updated_at
        
-- DBT SOURCE REFERENCE
FROM bronze.identity_management_users_roles_co AS ur

LEFT JOIN

-- DBT SOURCE REFERENCE
 bronze.identity_management_roles_permissions_co AS rp

ON ur.role_id=rp.role_id
GROUP BY ur.user_id,
         ur.role_id