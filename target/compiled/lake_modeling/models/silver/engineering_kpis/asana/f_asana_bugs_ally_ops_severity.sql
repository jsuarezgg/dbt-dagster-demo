

--bronze.asana_tasks
WITH removing_custom_fields AS (
    SELECT 
        gid, 
        custom_fields, 
        projects
    FROM bronze.asana_tasks
    WHERE custom_fields::string <> '[]' 
    AND array_contains(projects, '1202632056546796')
),
select_explode AS (
    SELECT 
        gid AS parent_task_gid,
        explode(custom_fields) AS cf_exploded
    FROM removing_custom_fields
)
SELECT DISTINCT
    parent_task_gid, 
    cf_exploded.display_value AS severity
FROM select_explode
WHERE cf_exploded.gid ='1202634040852815'