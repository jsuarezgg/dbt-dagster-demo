{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH app_events AS (
	SELECT
		user_id,
		event_date,
		COUNT(distinct session_id) AS app_sessions
	FROM {{ ref('dm_amplitude') }}
	WHERE 1=1
        AND upper(event_type) = 'APP_SCREEN_OPENED'
        AND upper(screen_name) = 'HOME'
        AND user_id IS NOT NULL 
        AND event_date >= '2022-09-01'
        AND upper(source) = 'MOBILE_APP'
	GROUP BY 1,2
)
, customer_segmentation AS (
	SELECT
		calculation_date,
		client_id,
		client_category,
		addicupo_state,
		app_sessions_in_calculation_month,
		app_sessions_last_30_days,
		LAG(client_category,7) OVER (PARTITION BY client_id ORDER BY calculation_date) AS prev_week_client_category
	FROM {{ source('gold', 'dm_daily_customer_segmentation_co') }} cs
	WHERE 1=1
			AND calculation_date > '2022-09-01'
) 
	SELECT
		calculation_date,
		client_category,
		prev_week_client_category,
		COUNT(distinct client_id) AS n_clients,
		COUNT(distinct client_id) FILTER (WHERE cs.addicupo_state = 'AVAILABLE') AS n_clients_cupo_available,
		COUNT(distinct client_id) FILTER (WHERE ae.app_sessions > 0) AS n_clients_opened_app,
		COUNT(distinct client_id) FILTER (WHERE cs.app_sessions_in_calculation_month > 0) AS n_clients_app_sessions_in_calculation_month,
		COUNT(distinct client_id) FILTER (WHERE cs.app_sessions_last_30_days > 0) AS n_clients_app_sessions_last_30_days
	FROM customer_segmentation cs
	LEFT JOIN app_events ae
		ON cs.calculation_date = ae.event_date
		AND cs.client_id = ae.user_id
	WHERE 1=1
		AND calculation_date > '2022-09-01'
	GROUP BY 1,2,3
