

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
    to_timestamp('2022-01-01') AS updated_at
FROM raw.data_costs_astro_tasks