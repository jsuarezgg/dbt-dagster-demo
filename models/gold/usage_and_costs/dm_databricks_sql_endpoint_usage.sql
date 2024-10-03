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
    "databricks_cost_by_sql_endpoint" AS metric_name,
    usage_date,
    sql_endpoint_name,
    SUM(databricks_price) AS databricks_price,
    SUM(aws_price) AS aws_price,
    SUM(total_price) AS total_price
FROM {{ ref('f_databricks_costs') }}
GROUP BY 2, 3;
