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
    timestamp::date AS usage_date,
    clusterNodeType AS instance_type,
    REGEXP_REPLACE(REGEXP_REPLACE(sku, '\\(', ''), '\\)', '') AS dbu_sku,
    dbus,
    machineHours AS aws_instance_machine_hours,
    clusterCustomTags:['SqlEndpointId'] AS sql_endpoint_id,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM {{ source('raw', 'data_costs_databricks_pricing') }}
