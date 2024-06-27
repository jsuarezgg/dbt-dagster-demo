

WITH base AS (

    SELECT
        sql_query,
        last_used_at,
        user_name,
        executed_as_user_name AS query_owner,
        explode(filter(
                regexp_extract_all(regexp_replace(sql_query, "--.*(?:\n|\r|$)", ""), "(?:from|join)\\s+([a-z0-9_\\.]+)\\b"),
                x -> x RLIKE '\\.'
            )) AS used_tables,
        rows_produced,
        duration_milliseconds/1000 AS duration_seconds,
        query_start_time,
        query_end_time,
        NOW() AS ingested_at,
        to_timestamp('2022-01-01') AS updated_at
    FROM bronze.databricks_usage_queries
    WHERE sql_query ILIKE '%from%'
    AND UPPER(status) = 'FINISHED'
    AND query_start_time >= current_date() - 30
    
)

SELECT
    sql_query,
    last_used_at,
    user_name,
    query_owner,
    used_tables,
    SPLIT_PART(used_tables,'.',1) AS schema_table,
    SPLIT_PART(used_tables,'.',2) AS table_name,
    rows_produced,
    duration_seconds,
    query_start_time,
    query_end_time,
    ingested_at,
    updated_at
FROM base