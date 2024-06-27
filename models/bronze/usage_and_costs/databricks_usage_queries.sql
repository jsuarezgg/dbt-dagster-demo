{{
    config(
        materialized='table',
        tags=["data_usage_and_costs"],
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

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
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM {{ source('raw', 'query_history_queries') }}
