

SELECT DISTINCT
    type AS instance_type,
    od_price AS instance_price_per_hour,
    spot_cost,
    saving_plan,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
FROM raw.data_costs_databricks_aws_instances_pricing