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
        a.amplitude_id AS user_id,
        upper(a.event_type) AS event_type,
        a.unique_id,
        from_utc_timestamp(to_timestamp(a.event_time),'America/Bogota') AS event_time,        
        COALESCE(upper(user_properties:['addiCupoStateV2']),upper(event_properties:['addiCupo.stateV2']),'PREAPPROVAL') AS cupo_state_v2
    FROM  {{ ref('f_amplitude_addi_funnel_project') }} a
    WHERE 1=1
        AND _year >= 2023
        AND _month >= 1
        AND (upper(event_type) IN ('HOME_STORE_TAPPED','SHOP_STORE_TAPPED','HOME_PRODUCT_TAPPED','HOME_PROMOTED_BANNER_TAPPED','SELECT_STORE','SELECT_DEAL')
            OR (upper(event_type) = 'APP_SCREEN_OPENED' AND upper(event_properties:['screenName']) NOT IN  ('WELCOME',
                                                                                                            'SELECT_COUNTRY',
                                                                                                            'OTP',
                                                                                                            'LOGIN_SELECTION',
                                                                                                            'SINGUP',
                                                                                                            'SINGUP_INSTRUCTIONS',
                                                                                                            'PROSPECT_LOGIN',
                                                                                                            'NETWORK_FAILED',
                                                                                                            'PHONE_NUMBER_FAILED',
                                                                                                            'ERROR_SCREEN',
                                                                                                            'CODE_PUSH')
                )
            )
        --AND COALESCE(upper( event_properties:['source']),upper(source)) = 'MOBILE_APP'
        AND event_date >  current_date() - interval '1 year'
        AND upper(platform) IN ('IOS','ANDROID')
        AND upper(country) = 'COLOMBIA'
        AND COALESCE(a.user_id, a.amplitude_id) IS NOT NULL
)
, shop_application AS (
    SELECT
        ap.client_id,
        ap.ally_slug,
        ap.application_id,
        ap.application_datetime_local,
        ap.is_addishop_referral,
        ap.is_addishop_referral_paid
    FROM {{ ref('dm_applications') }}  ap 
    WHERE 1=1
        AND application_datetime::date >  current_date() - interval '1 year'
        AND ap.is_addishop_referral IS TRUE
        AND ap.addishop_channel IN ('MOBILE_APP','APP')
)
, application_daily AS (
SELECT
  date_trunc('day',application_datetime_local) AS period,
  COUNT(distinct client_id) AS shop_users_applicating,
  COUNT(distinct client_id) FILTER (WHERE is_addishop_referral_paid IS TRUE) AS shop_users_paying_applicating,
  COUNT(distinct application_id) AS shop_applications,
  COUNT(distinct application_id) FILTER (WHERE is_addishop_referral_paid IS TRUE) AS shop_paying_applications
FROM shop_application
GROUP BY 1
)
, application_weekly AS (
SELECT
  date_trunc('week',application_datetime_local) AS period,
  COUNT(distinct client_id) AS shop_users_applicating,
  COUNT(distinct client_id) FILTER (WHERE is_addishop_referral_paid IS TRUE) AS shop_users_paying_applicating,
  COUNT(distinct application_id) AS shop_applications,
  COUNT(distinct application_id) FILTER (WHERE is_addishop_referral_paid IS TRUE) AS shop_paying_applications
FROM shop_application
GROUP BY 1
)
, application_monthly AS (
SELECT
  date_trunc('month',application_datetime_local) AS period,
  COUNT(distinct client_id) AS shop_users_applicating,
  COUNT(distinct client_id) FILTER (WHERE is_addishop_referral_paid IS TRUE) AS shop_users_paying_applicating,
  COUNT(distinct application_id) AS shop_applications,
  COUNT(distinct application_id) FILTER (WHERE is_addishop_referral_paid IS TRUE) AS shop_paying_applications
FROM shop_application
GROUP BY 1
)
, shop_loans AS (
  SELECT
    client_id,
    loan_id,
    application_id,
    origination_date_local,
    is_addishop_referral,
    is_addishop_referral_paid
  FROM  {{ ref('dm_originations') }}
  WHERE 1=1
    AND country_code = 'CO'
    AND addishop_channel IN ('APP','MOBILE_APP')
    AND origination_date >  current_date() - interval '1 year'
    AND is_addishop_referral IS TRUE
)
, originations_daily AS (
SELECT
  date_trunc('day',origination_date_local) AS period,
  COUNT(distinct client_id) AS shop_users_originating,
  COUNT(distinct client_id) FILTER (WHERE is_addishop_referral_paid IS TRUE) AS shop_users_paying_originating,
  COUNT(distinct application_id) AS shop_originations,
  COUNT(distinct application_id) FILTER (WHERE is_addishop_referral_paid IS TRUE) AS shop_paying_originations
FROM shop_loans
GROUP BY 1
)
, originations_weekly AS (
SELECT
  date_trunc('week',origination_date_local) AS period,
  COUNT(distinct client_id) AS  shop_users_originating,
  COUNT(distinct client_id) FILTER (WHERE is_addishop_referral_paid IS TRUE) AS shop_users_paying_originating,
  COUNT(distinct application_id) AS shop_originations,
  COUNT(distinct application_id) FILTER (WHERE is_addishop_referral_paid IS TRUE) AS shop_paying_originations
FROM shop_loans
GROUP BY 1
)
, originations_monthly AS (
SELECT
  date_trunc('month',origination_date_local) AS period,
  COUNT(distinct client_id) AS shop_users_originating,
  COUNT(distinct client_id) FILTER (WHERE is_addishop_referral_paid IS TRUE) AS shop_users_paying_originating,
  COUNT(distinct application_id) AS shop_originations,
  COUNT(distinct application_id) FILTER (WHERE is_addishop_referral_paid IS TRUE) AS shop_paying_originations
FROM shop_loans
GROUP BY 1
)
, app_daily AS (

SELECT
  date_trunc('day',event_time) AS period,
  COUNT(distinct user_id) AS app_users,
  COUNT(distinct user_id) FILTER (WHERE cupo_state_v2 IN ('AVAILABLE', 'NOT_ASSIGNED','PREAPPROVAL'))AS shop_app_users,
  COUNT(distinct user_id) FILTER (WHERE cupo_state_v2 IN ('AVAILABLE', 'NOT_ASSIGNED','PREAPPROVAL') AND event_type <> 'APP_SCREEN_OPENED' )AS shop_app_clickers
FROM app_events
GROUP BY 1
)
, app_weekly AS (

SELECT
  date_trunc('week',event_time) AS period,
  COUNT(distinct user_id) AS app_users,
  COUNT(distinct user_id) FILTER (WHERE cupo_state_v2 IN ('AVAILABLE', 'NOT_ASSIGNED','PREAPPROVAL') )AS shop_app_users,
  COUNT(distinct user_id) FILTER (WHERE cupo_state_v2 IN ('AVAILABLE', 'NOT_ASSIGNED','PREAPPROVAL') AND event_type <> 'APP_SCREEN_OPENED' )AS shop_app_clickers
FROM app_events
GROUP BY 1
)
, app_monthly AS (

SELECT
  date_trunc('month',event_time) AS period,
  COUNT(distinct user_id) AS app_users,
  COUNT(distinct user_id) FILTER (WHERE cupo_state_v2 IN ('AVAILABLE', 'NOT_ASSIGNED','PREAPPROVAL') )AS shop_app_users,
  COUNT(distinct user_id) FILTER (WHERE cupo_state_v2 IN ('AVAILABLE', 'NOT_ASSIGNED','PREAPPROVAL') AND event_type <> 'APP_SCREEN_OPENED' )AS shop_app_clickers
FROM app_events
GROUP BY 1
)
SELECT
  'daily' AS period_type,
  ad.*,
  shop_users_applicating,
  shop_users_paying_applicating,
  shop_applications,
  shop_paying_applications,
  shop_users_originating,
  shop_users_paying_originating,
  shop_originations,
  shop_paying_originations,
  NOW() AS ingested_at,
  to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM app_daily ad
LEFT JOIN application_daily a
  ON ad.period = a.period
LEFT JOIN originations_daily o
  ON ad.period = o.period

UNION ALL

SELECT
  'weekly' AS period_type,
  ad.*,
  shop_users_applicating,
  shop_users_paying_applicating,
  shop_applications,
  shop_paying_applications,
  shop_users_originating,
  shop_users_paying_originating,
  shop_originations,
  shop_paying_originations,
  NOW() AS ingested_at,
  to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM app_weekly ad
LEFT JOIN application_weekly a
  ON ad.period = a.period
LEFT JOIN originations_weekly o
  ON ad.period = o.period

UNION ALL

SELECT
  'monthly' AS period_type,
  ad.*,
  shop_users_applicating,
  shop_users_paying_applicating,
  shop_applications,
  shop_paying_applications,
  shop_users_originating,
  shop_users_paying_originating,
  shop_originations,
  shop_paying_originations,
  NOW() AS ingested_at,
  to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM app_monthly ad
LEFT JOIN application_monthly a
  ON ad.period = a.period
LEFT JOIN originations_monthly o
  ON ad.period = o.period
