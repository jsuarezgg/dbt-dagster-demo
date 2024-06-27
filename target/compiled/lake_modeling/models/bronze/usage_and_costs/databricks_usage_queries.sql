

SELECT
    user_name,
    executed_as_user_name,
    lower(query_text) as sql_query,
    duration AS duration_milliseconds,
    query_start_time as last_used_at,
    status,
    rows_produced,
    query_start_time,
    query_end_time,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
FROM raw.query_history_queries