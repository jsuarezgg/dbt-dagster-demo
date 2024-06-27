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
    usage_date,
    dag_id,
    SUM(num_successful_tasks) AS num_successful_tasks,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM {{ ref('astronomer_usage_data') }}
GROUP BY 1, 2
