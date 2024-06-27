{{
    config(
        materialized='table',
        tags=["data_usage_and_costs"],
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


WITH split_table AS (

    SELECT user_name,
            sql_query,
            SPLIT_PART(used_tables,'.',1) AS schema_table,
            SPLIT_PART(used_tables,'.',2) AS table_name,
            last_used_at,
            rows_produced,
            duration_seconds
    FROM {{ ref('f_databricks_usage_queries') }}

)

SELECT
    user_name,
    COUNT(DISTINCT CONCAT(sql_query,'-',last_used_at)) AS n_queries,
    MAX(last_used_at) AS last_user_run,
    AVG(rows_produced) AS avg_rows,
    AVG(duration_seconds) AS avg_duration_sec,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM split_table
WHERE schema_table <> "a"
GROUP BY 1
