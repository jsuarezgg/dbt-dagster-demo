{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

-- QUERY



WITH monthly_users_category AS (
  SELECT
    date_trunc('month',session_date) AS period,
    user_id,
    session_profile_category,
    count(*) AS sessions
  FROM {{ ref('dm_app_session_profile') }}
  WHERE 1=1
    --AND session_profile_category <> 'Other'
  GROUP BY ALL
)
, monthly_users_pivot AS (
  SELECT
      period,
      user_id,
      SUM(CASE WHEN session_profile_category IN ('Shop') THEN sessions ELSE 0 END) AS shop_sessions,
      SUM(CASE WHEN session_profile_category = 'Pay' THEN sessions ELSE 0  END) AS pay_sessions,
      SUM(CASE WHEN session_profile_category = 'Shop & Pay' THEN sessions ELSE 0  END) AS shop_pay_sessions,
      SUM(CASE WHEN session_profile_category = 'Other' THEN sessions ELSE 0  END) AS other_sessions
  FROM monthly_users_category
  GROUP BY ALL

)
, weekly_users_category AS (
  SELECT
    date_trunc('week',session_date) AS period,
    user_id,
    session_profile_category,
    count(*) AS sessions
  FROM {{ ref('dm_app_session_profile') }}
  WHERE 1=1
    --AND session_profile_category <> 'Other'
  GROUP BY ALL
)
, weekly_users_pivot AS (
  SELECT
      period,
      user_id,
      SUM(CASE WHEN session_profile_category IN ('Shop') THEN sessions ELSE 0 END) AS shop_sessions,
      SUM(CASE WHEN session_profile_category = 'Pay' THEN sessions ELSE 0  END) AS pay_sessions,
      SUM(CASE WHEN session_profile_category = 'Shop & Pay' THEN sessions ELSE 0  END) AS shop_pay_sessions,
      SUM(CASE WHEN session_profile_category = 'Other' THEN sessions ELSE 0  END) AS other_sessions
  FROM weekly_users_category
  GROUP BY ALL

)
, daily_users_category AS (
  SELECT
    session_date AS period,
    user_id,
    session_profile_category,
    count(*) AS sessions
  FROM {{ ref('dm_app_session_profile') }}
  WHERE 1=1
    --AND session_profile_category <> 'Other'
  GROUP BY ALL
)
, daily_users_pivot AS (
  SELECT
      period,
      user_id,
      SUM(CASE WHEN session_profile_category IN ('Shop') THEN sessions ELSE 0 END) AS shop_sessions,
      SUM(CASE WHEN session_profile_category = 'Pay' THEN sessions ELSE 0  END) AS pay_sessions,
      SUM(CASE WHEN session_profile_category = 'Shop & Pay' THEN sessions ELSE 0  END) AS shop_pay_sessions,
      SUM(CASE WHEN session_profile_category = 'Other' THEN sessions ELSE 0  END) AS other_sessions
  FROM daily_users_category
  GROUP BY ALL

)
  SELECT 
    'monthly' AS period_type,
    *, 
   CASE WHEN (shop_pay_sessions > 0 OR (pay_sessions > 0 AND shop_sessions > 0)) THEN 'Shop & Pay'
      WHEN pay_sessions > 0 AND shop_sessions = 0 THEN 'Pay'
      WHEN shop_sessions > 0 AND pay_sessions = 0 THEN 'Shop'
      ELSE 'Other'
    END AS user_profile
  FROM monthly_users_pivot

UNION ALL

  SELECT 
    'weekly' AS period_type,
    *, 
   CASE WHEN (shop_pay_sessions > 0 OR (pay_sessions > 0 AND shop_sessions > 0)) THEN 'Shop & Pay'
      WHEN pay_sessions > 0 AND shop_sessions = 0 THEN 'Pay'
      WHEN shop_sessions > 0 AND pay_sessions = 0 THEN 'Shop'
      ELSE 'Other'
    END AS user_profile
  FROM weekly_users_pivot

UNION ALL

 SELECT 
    'daily' AS period_type,
    *, 
  CASE WHEN (shop_pay_sessions > 0 OR (pay_sessions > 0 AND shop_sessions > 0)) THEN 'Shop & Pay'
      WHEN pay_sessions > 0 AND shop_sessions = 0 THEN 'Pay'
      WHEN shop_sessions > 0 AND pay_sessions = 0 THEN 'Shop'
      ELSE 'Other'
    END AS user_profile
  FROM daily_users_pivot
