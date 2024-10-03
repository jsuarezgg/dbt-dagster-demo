{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

SELECT
    'MONTHLY' as period_type,
    DATE_TRUNC('MONTH', sessions_date) AS period,
    UPPER(profile) AS profile,
    COUNT(DISTINCT amplitude_id) AS total_users,
    COUNT(DISTINCT CASE WHEN user_id IS NOT NULL THEN amplitude_id END) AS total_users_logged
FROM {{ ref('agg_app_monthly_profile') }} 
GROUP BY period_type, period, profile

UNION ALL

SELECT
    'WEEKLY' as period_type,
    DATE_TRUNC('WEEK', sessions_date) AS period,
    UPPER(profile) AS profile,
    COUNT(DISTINCT amplitude_id) AS total_users,
    COUNT(DISTINCT CASE WHEN user_id IS NOT NULL THEN amplitude_id END) AS total_users_logged
FROM {{ ref('agg_app_weekly_profile') }}
GROUP BY period_type, period, profile

UNION ALL

SELECT
    'DAILY' as period_type,
    DATE_TRUNC('DAY', sessions_date) AS period,
    UPPER(profile) AS profile,
    COUNT(DISTINCT amplitude_id) AS total_users,
    COUNT(DISTINCT CASE WHEN user_id IS NOT NULL THEN amplitude_id END) AS total_users_logged
FROM {{ ref('agg_app_daily_profile') }}
GROUP BY period_type, period, profile

ORDER BY period_type, period, profile