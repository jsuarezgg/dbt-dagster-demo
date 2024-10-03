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
    a.usage_date,
    a.instance_type,
    a.dbu_sku,
    a.dbus,
    b.dbu_price * (1 - 0.22) AS price_per_dbu,
    a.dbus * b.dbu_price * (1 - 0.22) AS databricks_price,
    a.aws_instance_machine_hours,
    c.instance_price_per_hour AS aws_price_per_hour,
    a.aws_instance_machine_hours * c.instance_price_per_hour AS aws_price,
    a.dbus * b.dbu_price * (1 - 0.22) + a.aws_instance_machine_hours * c.instance_price_per_hour AS total_price,
    CASE
        WHEN a.sql_endpoint_id = "916587995fc7f8df" THEN 'addi-analytics'
        WHEN a.sql_endpoint_id = "575e2c8435206de5" THEN 'BAs'
        WHEN a.sql_endpoint_id = "bf9f6b179664d9a7" THEN 'collections-ops'
        WHEN a.sql_endpoint_id = "35c63d074575f14f" THEN 'databricks_ae_communications_hf'
        WHEN a.sql_endpoint_id = "ca8106393d15c62f" THEN 'databricks_ae_risk_master_table_co'
        WHEN a.sql_endpoint_id = "faa155176f234996" THEN 'dbt-analytics'
        WHEN a.sql_endpoint_id = "6b20c2fb6df6ec06" THEN 'dbt-analytics2'
        WHEN a.sql_endpoint_id = "33e004bedc88ea98" THEN 'dbt-classic'
        WHEN a.sql_endpoint_id = "9edd7cb1687fe3e7" THEN 'looker'
        WHEN a.sql_endpoint_id = "4607dd45a4f2097e" THEN 'risk-sql-warehouse'
        WHEN a.sql_endpoint_id = "52cbdc82ba98a602" THEN 'Serverless Starter Endpoint'
        WHEN a.sql_endpoint_id = "6c19b5b2bb765c58" THEN 'Test Redash Migration'
        WHEN a.sql_endpoint_id IS NULL THEN NULL
        ELSE 'unknown'
    END AS sql_endpoint_name
FROM {{ ref('databricks_data_costs_report') }} a
LEFT JOIN {{ ref('databricks_dbu_prices') }} b ON a.dbu_sku = b.dbu_sku
LEFT JOIN {{ ref('databricks_aws_prices') }} c ON a.instance_type = c.instance_type
