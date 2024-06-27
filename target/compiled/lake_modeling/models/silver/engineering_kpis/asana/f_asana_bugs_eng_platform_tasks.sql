

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
FROM bronze.asana_tasks
WHERE array_contains(projects, '1202599857899492')