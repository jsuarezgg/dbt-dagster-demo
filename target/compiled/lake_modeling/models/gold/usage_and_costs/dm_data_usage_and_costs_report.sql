


WITH base_metrics AS (

    SELECT
        MAX("amplitude_number_of_events") AS metric_name,
        event_date AS metric_date,
        SUM(count_of_events) AS metric_value,
        MAX(NOW()) AS ingested_at,
        MAX('2022-01-01') AS updated_at
    FROM silver.f_amplitude_events_counts
    GROUP BY 2

    UNION ALL

    SELECT
        MAX("databricks_dbu_costs") AS metric_name,
        usage_date AS metric_date,
        SUM(databricks_price) AS metric_value,
        MAX(NOW()) AS ingested_at,
        MAX('2022-01-01') AS updated_at
    FROM silver.f_databricks_costs
    GROUP BY 2

    UNION ALL

    SELECT
        MAX("aws_databricks_costs") AS metric_name,
        usage_date AS metric_date,
        SUM(aws_price) AS metric_value,
        MAX(NOW()) AS ingested_at,
        MAX('2022-01-01') AS updated_at
    FROM silver.f_databricks_costs
    GROUP BY 2

    UNION ALL

    SELECT
        MAX("astronomer_number_of_tasks") AS metric_name,
        usage_date AS metric_date,
        SUM(num_successful_tasks) AS metric_value,
        MAX(NOW()) AS ingested_at,
        MAX('2022-01-01') AS updated_at
    FROM silver.f_astronomer_task_counts
    GROUP BY 2

),

join_table AS (

    SELECT
        metric_name,
        metric_value,
        metric_date,
        budget, 
        budget_usd,
        unit_cost,
        periodicity,
        start_date,
        end_date 
    FROM base_metrics duc
    INNER JOIN silver.d_data_platform_budget dpb 
    ON duc.metric_date::date BETWEEN dpb.start_date::date AND dpb.end_date::date
        AND SPLIT_PART(duc.metric_name,'_',1) = LOWER(dpb.platform)

),

flag_last_period AS (

    SELECT metric_name,
        MAX(start_date) AS last_start_date,
        true AS is_last_period
    FROM join_table
    GROUP BY metric_name, is_last_period

),

agg_table AS (

    SELECT
        jt.metric_name,
        metric_value,
        metric_value * unit_cost AS metric_usd_value,
        metric_date,
        budget, 
        ROUND(budget/datediff(end_date + INTERVAL '1 day',start_date),2) AS expected_metric,
        budget_usd, 
        ROUND(budget_usd/datediff(end_date + INTERVAL '1 day',start_date),2) AS expected_usd_metric,
        periodicity,
        start_date,
        end_date,
        CURRENT_DATE() - INTERVAL '1 day' AS last_date,
        datediff(end_date + INTERVAL '1 day',start_date) AS contract_days, 
        datediff(CASE WHEN CURRENT_DATE() - INTERVAL '1 day' <= end_date THEN CURRENT_DATE() ELSE end_date + INTERVAL '1 day' END,start_date) AS days_passed,
        COALESCE(is_last_period, false) AS is_last_period
    FROM join_table jt
    LEFT JOIN flag_last_period flp ON flp.metric_name = jt.metric_name AND flp.last_start_date = jt.start_date 

)

SELECT
    metric_name,
    metric_value, 
    COALESCE(metric_usd_value,metric_value) AS metric_usd_value,
    metric_date,
    ROUND(SUM(SUM(metric_value)) OVER (PARTITION BY metric_name, start_date, end_date ORDER BY metric_date),2) AS acum_metric, 
    ROUND(SUM(SUM(COALESCE(metric_usd_value,metric_value))) OVER (PARTITION BY metric_name, start_date, end_date ORDER BY metric_date),2) AS acum_usd_metric,
    budget, 
    budget_usd,
    expected_metric,
    expected_usd_metric,
    ROUND(SUM(SUM(expected_metric)) OVER (PARTITION BY metric_name, start_date, end_date ORDER BY metric_date),2) AS acum_expected_metric, 
    ROUND(SUM(SUM(expected_usd_metric)) OVER (PARTITION BY metric_name, start_date, end_date ORDER BY metric_date),2) AS acum_expected_usd_metric, 
    periodicity,
    start_date,
    end_date,
    last_date,
    contract_days, 
    days_passed,
    is_last_period
FROM agg_table
GROUP BY
    metric_name,
    metric_value,
    COALESCE(metric_usd_value,metric_value),
    metric_date,
    budget, 
    budget_usd,
    expected_metric,
    expected_usd_metric,
    periodicity,
    start_date,
    end_date,
    last_date,
    contract_days,
    days_passed,
    is_last_period