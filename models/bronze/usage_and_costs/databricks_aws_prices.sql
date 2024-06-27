{{
    config(
        materialized='table',
        tags=["data_usage_and_costs"],
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

SELECT DISTINCT
    type AS instance_type,
    od_price AS instance_price_per_hour,
    spot_cost,
    saving_plan,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM {{ source('raw', 'data_costs_databricks_aws_instances_pricing') }}
