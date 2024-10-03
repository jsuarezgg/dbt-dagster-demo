{% macro gold_agg_app_query_by_period(period) -%}
WITH base AS (
SELECT
  MAX(user_id) AS user_id,
  amplitude_id,
  MAX(role) AS role,
  MAX(cupo_state_v2) AS cupo_state_v2,
  MAX(addiCupoTotal) AS addiCupoTotal,
  --Min because sometimes the client is paying the debt in the session and this value is updated
  MIN(addiCupoUsed) AS addiCupoUsed,
  MAX(addiCupoBalance) AS addiCupoBalance,
  MAX(isTransactionalBased) AS isTransactionalBased,
  MAX(grande) AS grande,
  MAX(preapprovalExpirationDate) AS preapprovalExpirationDate,
  MAX(preapprovalAmount) AS preapprovalAmount,
  MAX(paymentStatus) AS paymentStatus,
  MAX(daysPastDue) AS daysPastDue,
  MAX(marketplace_default_screen) AS marketplace_default_screen,
  MIN(platform) AS platform,
  MAX(version_name) AS version_name,
  MAX(city) AS city,
  date_trunc('{{period}}',session_start_time) AS sessions_date,
  SUM(session_duration_secs) AS session_duration_secs,
  COUNT(distinct session_id) AS total_sessions,
  COLLECT_list(session_id) AS session_id_list,
  SUM(total_events) AS total_events,
  SUM(shop_home_seen) AS shop_home_seen,
  SUM(shop_browsing) AS shop_browsing,
  SUM(shop_search) AS shop_search,
  SUM(shop_intention) AS shop_intention,
  SUM(deal_curiosity) AS deal_curiosity,
  SUM(deal_clicked) AS deal_clicked,
  SUM(marketplace_seen) AS marketplace_seen,
  SUM(marketplace_browsing) AS marketplace_browsing,
  SUM(marketplace_search) AS marketplace_search,
  SUM(marketplace_product_interaction) AS marketplace_product_interaction,
  SUM(marketplace_product_add_to_cart) AS marketplace_product_add_to_cart,
  SUM(marketplace_checkout_flow) AS marketplace_checkout_flow,
  SUM(marketplace_purchase_confirmed) AS marketplace_purchase_confirmed,
  SUM(nequi_configuration) AS nequi_configuration,
  SUM(pay_curiosity) AS pay_curiosity,
  SUM(pay_intention) AS pay_intention,
  SUM(pay_feedback) AS pay_feedback,
  SUM(purchases_curiosity) AS purchases_curiosity,
  SUM(settings_clicked) AS settings_clicked
FROM {{ ref('dm_app_session_profile') }}
GROUP BY ALL
)
SELECT
*,
CASE WHEN shop_browsing + shop_search + shop_intention + deal_curiosity + deal_clicked + marketplace_browsing + marketplace_search + marketplace_product_interaction + marketplace_product_add_to_cart +  marketplace_checkout_flow + marketplace_purchase_confirmed + nequi_configuration + pay_curiosity + pay_intention + pay_feedback + purchases_curiosity = 0 AND shop_home_seen > 0 THEN 'Check Cupo'
WHEN 
  (shop_browsing + shop_search + shop_intention + deal_curiosity + deal_clicked + marketplace_browsing + marketplace_search + marketplace_product_interaction + marketplace_product_add_to_cart +  marketplace_checkout_flow + marketplace_purchase_confirmed = 0) 
  AND nequi_configuration + pay_curiosity + pay_intention + pay_feedback > 0 THEN 'Pay'
WHEN 
  (shop_browsing + shop_search + shop_intention + deal_curiosity + deal_clicked + marketplace_browsing + marketplace_search + marketplace_product_interaction + marketplace_product_add_to_cart +  marketplace_checkout_flow + marketplace_purchase_confirmed > 0) 
  AND nequi_configuration + pay_curiosity + pay_intention + pay_feedback = 0 THEN 'Shop'
WHEN 
  (shop_browsing + shop_search + shop_intention + deal_curiosity + deal_clicked + marketplace_browsing + marketplace_search + marketplace_product_interaction + marketplace_product_add_to_cart +  marketplace_checkout_flow + marketplace_purchase_confirmed > 0) 
  AND nequi_configuration + pay_curiosity + pay_intention + pay_feedback > 0 THEN 'Shop & Pay'
WHEN shop_browsing + shop_search + shop_intention + deal_curiosity + deal_clicked + marketplace_browsing + marketplace_search + marketplace_product_interaction + marketplace_product_add_to_cart +  marketplace_checkout_flow + marketplace_purchase_confirmed + nequi_configuration + pay_curiosity + pay_intention + pay_feedback + purchases_curiosity = 0 AND marketplace_seen > 0 THEN 'Marketplace seen - No interaction'
ELSE 'other' END AS profile
  FROM
    base
{%- endmacro %}
