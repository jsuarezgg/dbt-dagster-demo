

--bronze.asana_tasks
SELECT DISTINCT
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    gid,
    assignee, 
    completed, 
    completed_at, 
    completed_by, 
    created_at, 
    name, 
    permalink_url
FROM bronze.asana_tasks
WHERE array_contains(projects, '1203414502507925')