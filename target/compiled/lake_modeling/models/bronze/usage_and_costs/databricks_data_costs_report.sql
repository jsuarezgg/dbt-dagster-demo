

SELECT DISTINCT
    timestamp::date AS usage_date,
    clusterNodeType AS instance_type,
    REGEXP_REPLACE(REGEXP_REPLACE(sku, '\\(', ''), '\\)', '') AS dbu_sku,
    dbus,
    machineHours AS aws_instance_machine_hours,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
FROM raw.data_costs_databricks_pricing