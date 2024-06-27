

SELECT
    usage_date,
    dag_id,
    SUM(num_successful_tasks) AS num_successful_tasks,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
FROM bronze.astronomer_usage_data
GROUP BY 1, 2