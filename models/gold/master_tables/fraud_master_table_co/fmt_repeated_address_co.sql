{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH apps_data AS (
    SELECT a.application_id,
    LOWER(a.fixed_address) AS fixed_address,
    orig.origination_date,
    orig.client_id
    FROM {{ source('silver_live', 'f_originations_bnpl_co') }} orig
    LEFT JOIN {{ source('gold', 'fmt_clean_address_apps_co') }} a ON a.application_id = orig.application_id
    WHERE orig.loan_id IS NOT NULL
    AND a.fixed_address IS NOT NULL
),
diff_prev_clients AS (
	SELECT
		*,
		max(client_id) OVER(PARTITION BY fixed_address ORDER BY origination_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_prev_client_id,
    min(client_id) OVER(PARTITION BY fixed_address ORDER BY origination_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_prev_client_id
	FROM apps_data
)
SELECT
    application_id,
    CASE WHEN max_prev_client_id IS DISTINCT FROM min_prev_client_id THEN 1 ELSE 0 END AS repeated_address
FROM diff_prev_clients
