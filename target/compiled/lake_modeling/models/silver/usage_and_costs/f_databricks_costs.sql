

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
    a.dbus * b.dbu_price * (1 - 0.22) + a.aws_instance_machine_hours * c.instance_price_per_hour AS total_price
FROM bronze.databricks_data_costs_report a
LEFT JOIN bronze.databricks_dbu_prices b ON a.dbu_sku = b.dbu_sku
LEFT JOIN bronze.databricks_aws_prices c ON a.instance_type = c.instance_type