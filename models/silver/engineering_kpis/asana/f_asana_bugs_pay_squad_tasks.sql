{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--bronze.asana_tasks
SELECT DISTINCT
    gid,
    assignee, 
    completed, 
    completed_at, 
    completed_by, 
    created_at, 
    name, 
    permalink_url
FROM {{ source('bronze', 'asana_tasks') }}
WHERE array_contains(projects, '1205207223265253')