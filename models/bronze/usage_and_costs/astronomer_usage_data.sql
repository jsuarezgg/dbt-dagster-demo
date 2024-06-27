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
    `date` AS usage_date,
    dag_id,
    dag_run_id,
    num_tasks,
    num_successful_tasks,
    workspace,
    deployment,
    deployment_id,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM {{ source('raw', 'data_costs_astro_tasks') }}
