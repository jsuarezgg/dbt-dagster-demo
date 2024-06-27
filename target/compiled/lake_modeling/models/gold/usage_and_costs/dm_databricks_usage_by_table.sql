


WITH split_table AS (

    SELECT user_name,
            sql_query,
            SPLIT_PART(used_tables,'.',1) AS schema_table,
            SPLIT_PART(used_tables,'.',2) AS table_name,
            last_used_at,
            rows_produced,
            duration_seconds
    FROM silver.f_databricks_usage_queries

)

SELECT
    schema_table,
    table_name,
    COUNT(table_name) AS n_uses,
    MAX(last_used_at) AS last_use
FROM split_table
WHERE schema_table <> "a"
GROUP BY 1,2