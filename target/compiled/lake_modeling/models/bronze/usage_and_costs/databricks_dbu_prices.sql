

SELECT DISTINCT
    CASE WHEN UPPER(CONCAT(plan, "_",REPLACE(REPLACE(compute, " ", "_"), "-", "_"))) = "PREMIUM_SQL_SERVERLESS_COMPUTE"
        THEN "PREMIUM_SERVERLESS_SQL_COMPUTE_US_EAST_N_VIRGINIA"
        ELSE UPPER(CONCAT(plan, "_",REPLACE(REPLACE(compute, " ", "_"), "-", "_")))
    END AS dbu_sku,
    baserate AS dbu_price,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
FROM raw.data_costs_databricks_costs_dbu
WHERE region LIKE "US East%" OR region IS NULL OR region = "n/a"