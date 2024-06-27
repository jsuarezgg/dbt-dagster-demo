{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

-- QUERY


WITH app_opened AS (
--Keep user_id that opened the app
  SELECT distinct user_id
  FROM {{ ref('f_amplitude_addi_funnel_project') }} a
  WHERE 1=1
      AND upper(a.platform) IN ('IOS','ANDROID')
      AND COALESCE(upper( event_properties:['source']),upper(source)) = 'MOBILE_APP'
      --AND upper(country) = 'COLOMBIA'
      AND date_trunc('month',event_date)  >  current_date() - interval '1 year'
      --event_properties:['selectedTab']
      AND user_id IS NOT NULL
      AND event_type = 'APP_SCREEN_OPENED'
      AND upper(cast(get_json_object(event_properties,'$.screenName') as string)) NOT IN  ('WELCOME',
                                                                                            'SELECT_COUNTRY',
                                                                                            'OTP',
                                                                                            'LOGIN_SELECTION',
                                                                                            'SINGUP',
                                                                                            'SINGUP_INSTRUCTIONS',
                                                                                            'PROSPECT_LOGIN',
                                                                                            'NETWORK_FAILED',
                                                                                            'PHONE_NUMBER_FAILED',
                                                                                            'ERROR_SCREEN')
      --AND upper(cast(get_json_object(event_properties,'$.screenName') as string)) NOT IN ('WELCOME', 'OTP','SELECT_COUNTRY')
      AND  COALESCE(upper(user_properties:['addiCupoStateV2']),upper(event_properties:['addiCupo.stateV2']),'PREAPPROVAL') IN ('NOT_ASSIGNED','AVAILABLE','PREAPPROVAL')
) 
, app_events_profile AS (
    SELECT
        a.user_id,
        a.session_id,
        upper(country) AS country,
        MIN(event_date) AS session_date, 
        MAX(COALESCE(upper(user_properties:['addiCupoStateV2']),upper(event_properties:['addiCupo.stateV2']),'PREAPPROVAL')) AS cupo_state_v2,
        SUM(CASE WHEN (upper(event_type) = 'SHOP_SEARCH_BAR_TAPPED' 
                      OR (upper(event_type) = 'APP_NAVBAR_BUTTON_TAPPED' AND event_properties:['selectedTab'] = 'SHOP_NOW')) 
            THEN 1 ELSE 0 END) AS shopping_search,
        SUM(CASE WHEN (upper(event_type) IN (  'SHOP_CATEGORY_TAPPED',
                                          'HOME_FEATURED_SECTION_SEE_ALL_TAPPED',
                                          'SUBCATEGORY TAPPED',
                                          'HOME_SEE_ALL_CATEGORIES_BUTTON_TAPPED',
                                          'HOME_SEE_ALL_STORES_BUTTON_TAPPED',
                                          'SELECT SORT ALLIES') OR (upper(event_type) = 'APP_SCREEN_OPENED' AND upper(cast(get_json_object(event_properties,'$.screenName') as string)) IN ('SUBCATEGORY','CATEGORY','CATEGORIES','SUB_CATEGORY','STORES'))
                                          OR (upper(event_type)IN('APP_NAVBAR_BUTTON_TAPPED') AND event_properties:['selectedTab'] = 'DEALS')) THEN 1 ELSE 0 END) AS shopping_browse,
        SUM(CASE WHEN upper(event_type) IN ('HOME_STORE_TAPPED',
                                          'SHOP_STORE_TAPPED',
                                          'SELECT_STORE',
                                          'SELECT_DEAL',
                                          'HOME_PROMOTED_BANNER_TAPPED') THEN 1 ELSE 0 END) AS shopping_intention,
        SUM(CASE WHEN (upper(event_type) IN ('PAYMENTS_CO_CONTINUE_BUTTON_TAPPED',
                                          'PAYMENTS_CO_AMOUT_TO_PAY_SELECTED',
                                          'PAYMENTS_CO_PSE_CONTINUE_BUTTON_TAPPED',
                                          'PAYMENTS_CO_METHOD_SELECTED',
                                          'PAYMENTS_CO_CORRESPONSAL_HOME_SCREEN_TAPPED',
                                          'PAYMENTS_CO_CORRESPONSAL_DOWNLOAD_RECEIPT_TAPPED',
                                          'PAYMENTS_CO_USE_ANOTHER_METHOD_TAPPED') 
                                           OR (upper(event_type) IN ('APP_SCREEN_OPENED') AND upper(cast(get_json_object(event_properties,'$.screenName') as string)) IN ('PAYMENT_AMOUNT','PAYMENT_METHOD','PAYMENT_METHODS','PSE')))
                                           THEN 1 ELSE 0 END) AS payment_intention,   
  SUM(CASE WHEN (upper(event_type) IN ('PAYMENTS_PAY_BUTTON_TAPPED',
                                          'PAYMENTS_TAB_SELECTED',
                                          'PAYMENTS_PURCHASE_HISTORY_TAPPED',
                                          'PAYMENTS_HISTORY_TAPPED',
                                          'APP_VIEW_HISTORY_BUTTON_TAPPED',
                                          'PAYMENTS_INFO_BUTTON_TAPPED',
                                          'PAYMENTS_CO_SAVED_PAYMENT_TAPPED')
                                          OR (upper(event_type)IN('APP_NAVBAR_BUTTON_TAPPED') AND event_properties:['selectedTab'] = 'PURCHASES')
                                         OR (upper(event_type)IN('APP_SCREEN_OPENED') AND upper(cast(get_json_object(event_properties,'$.screenName') as string)) IN ('PURCHASES'))
                                          )THEN 1 ELSE 0 END) AS payment_curiosity
    FROM {{ ref('f_amplitude_addi_funnel_project') }} a
    WHERE 1=1
        AND upper(a.platform) IN ('IOS','ANDROID')
        AND COALESCE(upper(user_properties:['addiCupoStateV2']),upper(event_properties:['addiCupo.stateV2']),'PREAPPROVAL') IN ('NOT_ASSIGNED','AVAILABLE','PREAPPROVAL')
        AND upper(cast(get_json_object(event_properties,'$.screenName') as string)) NOT IN  ('WELCOME',
                                                                                            'SELECT_COUNTRY',
                                                                                            'OTP',
                                                                                            'LOGIN_SELECTION',
                                                                                            'SINGUP',
                                                                                            'SINGUP_INSTRUCTIONS',
                                                                                            'PROSPECT_LOGIN',
                                                                                            'NETWORK_FAILED',
                                                                                            'PHONE_NUMBER_FAILED',
                                                                                            'ERROR_SCREEN')
        AND COALESCE(upper( event_properties:['source']),upper(source)) = 'MOBILE_APP'
        --AND upper(country) = 'COLOMBIA'
         AND date_trunc('month',event_date)  > current_date() - interval '1 year'
        --event_properties:['selectedTab']
        --AND event_time > '2023-04-01'
        AND user_id IN (SELECT user_id FROM app_opened)
          --and session_id = '1693613313022'
         /*AND event_type IN (
                    --'APP_SCREEN_OPENED',
                    'HOME_STORE_TAPPED',
                    'APP_NAVBAR_BUTTON_TAPPED', 
                    'SHOP_SEARCH_BAR_TAPPED', 
                    'PAYMENTS_PAY_BUTTON_TAPPED', 
                    'PAYMENTS_CO_CONTINUE_BUTTON_TAPPED', 
                    'PAYMENTS_TAB_SELECTED',
                    'PAYMENTS_CO_PSE_CONTINUE_BUTTON_TAPPED',
                    'SHOP_CATEGORY_TAPPED', 
                    'PAYMENTS_PURCHASE_HISTORY_TAPPED',
                    --'HOME_RECENTLY_VISITED_ALLY_TAPPED', 
                    'PAYMENTS_CO_METHOD_SELECTED',
                    'HOME_SETTINGS_BUTTON_TAPPED',
                    'PAYMENTS_CO_TRANSACTION_SUCCESS',
                    'HOME_FEATURED_SECTION_ALLY_TAPPED',
                    'HOME_FEATURED_SECTION_SEE_ALL_TAPPED',
                    'PAYMENTS_HISTORY_TAPPED',
                    'HOME_PROMOTED_BANNER_TAPPED',
                    'HOME_SEE_ALL_CATEGORIES_BUTTON_TAPPED', 
                    'HOME_SELECT_FAVORITE_STORE',
                    'PAYMENTS_CO_CORRESPONSAL_DOWNLOAD_RECEIPT_TAPPED',
                    'PAYMENTS_CO_SAVED_PAYMENT_TAPPED',
                    'PAYMENTS_INFO_BUTTON_TAPPED',
                    'HOME_SEE_ALL_STORES_BUTTON_TAPPED',
                    'SHOP_STORE_TAPPED',
                    'SETTINGS_NOTIFICATION_TOGGLE_SWITCHED',
                    'NOTIFICATIONS_POP_UP_TAPED'
                    --'SETTINGS_NOTIFICATION_MODAL_TAPPED'
                    ) */
    GROUP BY ALL
)
, session_category AS (
  SELECT
    *,
    CASE WHEN shopping_browse > 0 AND shopping_search + shopping_intention + payment_intention + payment_curiosity = 0 THEN 'Browse'
    WHEN  shopping_search > 0 AND (shopping_intention + payment_intention + payment_curiosity = 0) THEN 'Search'
    WHEN shopping_intention > 0 AND payment_intention + payment_curiosity = 0 THEN 'Shop Intention'
    WHEN payment_curiosity > 0 AND shopping_search + shopping_intention + payment_intention + shopping_browse = 0 THEN 'Payment Curiosity'
    WHEN payment_intention > 0 AND shopping_search + shopping_intention +  shopping_browse = 0 THEN 'Payment Intention'
    WHEN shopping_browse > 0 AND payment_curiosity  > 0 AND  shopping_search + shopping_intention +  payment_intention = 0 THEN 'Pay Curiosity / Shop Browse'
    WHEN shopping_search > 0 AND payment_curiosity  > 0 AND  shopping_intention +  payment_intention = 0 THEN 'Pay Curiosity / Shop Search'
    WHEN shopping_intention > 0 AND payment_curiosity  > 0 AND  payment_intention = 0 THEN 'Pay Curiosity / Shop Intent'
    WHEN shopping_browse > 0 AND payment_intention > 0 AND  shopping_search + shopping_intention = 0 THEN 'Pay Intent / Shop Browse'
    WHEN shopping_search > 0 AND payment_intention  > 0 AND  shopping_intention  = 0 THEN 'Pay Intent / Shop Search'
    WHEN shopping_intention > 0 AND payment_intention  > 0  THEN 'Pay Intent / Shop Intent'
    ELSE 'Other' END AS session_profile
  FROM app_events_profile
)
SELECT 
  *,
  CASE WHEN session_profile IN ('Browse','Shop Intention', 'Search') THEN 'Shop'
      WHEN session_profile IN ('Payment Curiosity','Payment Intention') THEN 'Pay'
      WHEN session_profile = 'Other' THEN session_profile
  ELSE 'Shop & Pay' END AS session_profile_category
FROM session_category
