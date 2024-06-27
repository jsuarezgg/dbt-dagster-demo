

SELECT 
  split_part(table_name,'.',1) as schema_table,
  split_part(table_name,'.',2) as table_name,
  count(distinct sql_query) as number
FROM silver.f_databricks_usage_queries
WHERE table_name NOT like '(%' 
AND table_name != ' '
AND table_name NOT like '%timestamp%'
AND table_name LIKE '%.%'
GROUP BY 1,2
ORDER BY 3 desc