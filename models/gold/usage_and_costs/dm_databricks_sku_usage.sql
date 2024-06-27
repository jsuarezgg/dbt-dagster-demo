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
    "databricks_cost_by_sku" AS metric_name,
    usage_date,
    dbu_sku,
    SUM(databricks_price) AS metric_value
FROM silver.f_databricks_costs
GROUP BY 2, 3;
