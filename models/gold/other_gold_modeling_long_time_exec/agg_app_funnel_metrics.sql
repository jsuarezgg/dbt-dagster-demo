{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


WITH monthly_app_profiles AS (
  SELECT 
    DATE_TRUNC('MONTH', sessions_date) AS period,

    --App Users
    COUNT(DISTINCT amplitude_id) AS users_total,
    COUNT(DISTINCT amplitude_id) filter (where user_id IS NOT NULL) AS users_logged,
    COUNT(DISTINCT amplitude_id) filter (where user_id IS NULL) AS users_guest,

    --Shop Behavior
    COUNT(DISTINCT amplitude_id) filter (where profile IN ('Shop','Shop & Pay')) as users_shop_behavior, 
    COUNT(DISTINCT amplitude_id) filter (where user_id IS NOT NULL and profile IN ('Shop','Shop & Pay')) as users_logged_shop_behavior,
    COUNT(DISTINCT amplitude_id) filter (where user_id IS NULL and profile IN ('Shop','Shop & Pay')) as users_guest_shop_behavior,
    

    --Marketplace Behavior 
    COUNT(DISTINCT amplitude_id) filter (where  marketplace_browsing > 0 or marketplace_product_interaction > 0 or marketplace_product_add_to_cart >0 or marketplace_search >0 or marketplace_checkout_flow>0) as users_marketplace_behavior,
    COUNT(DISTINCT amplitude_id) filter (where (marketplace_browsing > 0 or marketplace_product_interaction > 0 or marketplace_product_add_to_cart >0 or marketplace_search >0 or marketplace_checkout_flow>0) and user_id IS NOT NULL ) as users_logged_marketplace_behavior,
    COUNT(DISTINCT amplitude_id) filter (where (marketplace_browsing > 0 or marketplace_product_interaction > 0 or marketplace_product_add_to_cart >0 or marketplace_search >0 or marketplace_checkout_flow>0) and user_id IS NULL ) as users_guest_marketplace_behavior,


    -- Referral Behavior 
    COUNT(DISTINCT amplitude_id) filter (where  shop_browsing > 0 or shop_search > 0 or shop_intention >0 or deal_curiosity >0 or deal_clicked>0) as users_referral_behavior,
    COUNT(DISTINCT amplitude_id) filter (where (shop_browsing > 0 or shop_search > 0 or shop_intention >0 or deal_curiosity >0 or deal_clicked>0) and user_id IS NOT NULL ) as users_logged_referral_behavior,
    COUNT(DISTINCT amplitude_id) filter (where (shop_browsing > 0 or shop_search > 0 or shop_intention >0 or deal_curiosity >0 or deal_clicked>0) and user_id IS NULL ) as users_guest_referral_behavior,

    
    --Shop Intention
    COUNT(DISTINCT amplitude_id) filter (where shop_intention > 0 or marketplace_product_interaction > 0 or marketplace_product_add_to_cart > 0 or marketplace_checkout_flow > 0) as users_shop_intention,
    COUNT(DISTINCT amplitude_id) filter (where (shop_intention > 0 or marketplace_product_interaction > 0 or marketplace_product_add_to_cart > 0 or marketplace_checkout_flow > 0)
                                              and user_id IS NOT NULL) as users_logged_shop_intention, 
    COUNT(DISTINCT amplitude_id) filter (where (shop_intention > 0 or marketplace_product_interaction > 0 or marketplace_product_add_to_cart > 0 or marketplace_checkout_flow > 0)
                                              and user_id IS NULL) as users_guest_shop_intention,

    --Marketplace seen
    COUNT(DISTINCT amplitude_id) filter (where marketplace_seen > 0) as users_marketplace_seen,
    COUNT(DISTINCT amplitude_id) filter (where marketplace_seen > 0 and user_id IS NOT NULL) as users_logged_marketplace_seen,
    COUNT(DISTINCT amplitude_id) filter (where marketplace_seen > 0 and user_id IS NULL) as users_guest_marketplace_seen,

    --Marketplace browsing
    COUNT(DISTINCT amplitude_id) filter (where marketplace_browsing > 0) as users_marketplace_browsing,
    COUNT(DISTINCT amplitude_id) filter (where marketplace_browsing > 0 and user_id IS NOT NULL) as users_logged_marketplace_browsing,
    COUNT(DISTINCT amplitude_id) filter (where marketplace_browsing > 0 and user_id IS NULL) as users_guest_marketplace_browsing,
    
    --Product Interaction
    COUNT(DISTINCT amplitude_id) filter (where marketplace_product_interaction > 0) as users_product_interaction,
    COUNT(DISTINCT amplitude_id) filter (where marketplace_product_interaction > 0 and user_id IS NOT NULL) as users_logged_product_interaction,
    COUNT(DISTINCT amplitude_id) filter (where marketplace_product_interaction > 0 and user_id IS NULL) as users_guest_product_interaction,

    --Add to cart
    COUNT(DISTINCT amplitude_id) filter (where marketplace_product_add_to_cart > 0) as users_add_to_cart,
    COUNT(DISTINCT amplitude_id) filter (where marketplace_product_add_to_cart > 0 and user_id IS NOT NULL) as users_logged_add_to_cart,
    COUNT(DISTINCT amplitude_id) filter (where marketplace_product_add_to_cart > 0 and user_id IS NULL) as users_guest_add_to_cart,

    --Marketplace Checkout flow
    COUNT(DISTINCT amplitude_id) filter (where marketplace_checkout_flow > 0) as users_mktplace_checkout,
    COUNT(DISTINCT amplitude_id) filter (where marketplace_checkout_flow > 0 and user_id IS NOT NULL) as users_logged_mktplace_checkout,
    COUNT(DISTINCT amplitude_id) filter (where marketplace_checkout_flow > 0 and user_id IS NULL) as users_guest_mktplace_checkout,


    -- Store Clicked 
    COUNT(DISTINCT amplitude_id) filter (where  shop_intention>0 or deal_clicked>0) AS users_store_clicked,
    COUNT(DISTINCT amplitude_id) filter (where (shop_intention>0 or deal_clicked>0) and user_id IS NOT NULL) AS users_logged_store_clicked,
    COUNT(DISTINCT amplitude_id) filter (where (shop_intention>0 or deal_clicked>0) and user_id IS NULL) AS users_guest_store_clicked


    
  FROM  {{ ref('agg_app_monthly_profile') }} a
  WHERE 1=1
    AND year(sessions_date) >= 2023
  GROUP BY 1
),
monthly_transactions AS (
  SELECT
    DATE_TRUNC('MONTH', a.application_date_local) AS period,
    
    --Applications
    COUNT(DISTINCT a.application_id) FILTER (WHERE a.is_addishop_referral = TRUE or a.ally_slug = 'addi-marketplace') AS shop_applications,
    COUNT(DISTINCT a.application_id) FILTER (WHERE a.is_addishop_referral = TRUE) AS referral_applications,
    COUNT(DISTINCT a.application_id) FILTER (WHERE a.is_addishop_referral = TRUE AND a.is_addishop_referral_paid = TRUE) AS referral_paid_applications,
    COUNT(DISTINCT a.application_id) FILTER (WHERE a.ally_slug = 'addi-marketplace') AS mktplace_applications,
    
    --Clients Applying
    COUNT(DISTINCT a.client_id) FILTER (WHERE a.is_addishop_referral = TRUE or a.ally_slug = 'addi-marketplace') AS users_shop_applying,
    COUNT(DISTINCT a.client_id) FILTER (WHERE a.is_addishop_referral = TRUE) AS referral_users_applying,
    COUNT(DISTINCT a.client_id) FILTER (WHERE a.is_addishop_referral = TRUE AND a.is_addishop_referral_paid = TRUE) AS referral_paid_users_applying,
    COUNT(DISTINCT a.client_id) FILTER (WHERE a.ally_slug = 'addi-marketplace') AS mktplace_users_applying,
    
    --Originations
    COUNT(DISTINCT b.application_id) FILTER (WHERE a.is_addishop_referral = TRUE or a.ally_slug = 'addi-marketplace') as shop_originations,
    COUNT(DISTINCT b.application_id) FILTER (WHERE a.is_addishop_referral = TRUE) as referral_originations,
    COUNT(DISTINCT b.application_id) FILTER (WHERE a.is_addishop_referral = TRUE AND a.is_addishop_referral_paid = TRUE) as referral_paid_originations,
    COUNT(DISTINCT b.application_id) FILTER (WHERE a.ally_slug = 'addi-marketplace') as mktplace_originations,

    --Clients originating
    COUNT(DISTINCT b.client_id) FILTER (WHERE a.is_addishop_referral = TRUE or a.ally_slug = 'addi-marketplace') as users_shop_originating,
    COUNT(DISTINCT b.client_id) FILTER (WHERE a.is_addishop_referral = TRUE) as referral_users_originating,
    COUNT(DISTINCT b.client_id) FILTER (WHERE a.is_addishop_referral = TRUE AND a.is_addishop_referral_paid = TRUE) as referral_paid_users_originating,
    COUNT(DISTINCT b.client_id) FILTER (WHERE a.ally_slug = 'addi-marketplace') as mktplace_users_originating
  FROM {{ ref('dm_applications') }} a
  LEFT JOIN {{ ref('dm_originations') }} b
    ON a.application_id = b.application_id
  WHERE 1=1
    AND year(a.application_date_local) >= 2023
  GROUP BY 1
),
weekly_app_profiles AS (
  SELECT 
    DATE_TRUNC('WEEK', sessions_date) AS period,

    --App Users
    COUNT(DISTINCT amplitude_id) AS users_total,
    COUNT(DISTINCT amplitude_id) filter (where user_id IS NOT NULL) AS users_logged,
    COUNT(DISTINCT amplitude_id) filter (where user_id IS NULL) AS users_guest,

    --Shop Behavior
    COUNT(DISTINCT amplitude_id) filter (where profile IN ('Shop','Shop & Pay')) as users_shop_behavior, 
    COUNT(DISTINCT amplitude_id) filter (where user_id IS NOT NULL and profile IN ('Shop','Shop & Pay')) as users_logged_shop_behavior,
    COUNT(DISTINCT amplitude_id) filter (where user_id IS NULL and profile IN ('Shop','Shop & Pay')) as users_guest_shop_behavior,
    

    --Marketplace Behavior 
    COUNT(DISTINCT amplitude_id) filter (where  marketplace_browsing > 0 or marketplace_product_interaction > 0 or marketplace_product_add_to_cart >0 or marketplace_search >0 or marketplace_checkout_flow>0) as users_marketplace_behavior,
    COUNT(DISTINCT amplitude_id) filter (where (marketplace_browsing > 0 or marketplace_product_interaction > 0 or marketplace_product_add_to_cart >0 or marketplace_search >0 or marketplace_checkout_flow>0) and  user_id IS NOT NULL ) as users_logged_marketplace_behavior,
    COUNT(DISTINCT amplitude_id) filter (where (marketplace_browsing > 0 or marketplace_product_interaction > 0 or marketplace_product_add_to_cart >0 or marketplace_search >0 or marketplace_checkout_flow>0) and  user_id IS NULL ) as users_guest_marketplace_behavior,


    -- Referral Behavior 
    COUNT(DISTINCT amplitude_id) filter (where  shop_browsing > 0 or shop_search > 0 or shop_intention >0 or deal_curiosity >0 or deal_clicked>0) as users_referral_behavior,
    COUNT(DISTINCT amplitude_id) filter (where (shop_browsing > 0 or shop_search > 0 or shop_intention >0 or deal_curiosity >0 or deal_clicked>0) and user_id IS NOT NULL ) as users_logged_referral_behavior,
    COUNT(DISTINCT amplitude_id) filter (where (shop_browsing > 0 or shop_search > 0 or shop_intention >0 or deal_curiosity >0 or deal_clicked>0) and user_id IS NULL ) as users_guest_referral_behavior,

    
    --Shop Intention
    COUNT(DISTINCT amplitude_id) filter (where shop_intention > 0 or marketplace_product_interaction > 0 or marketplace_product_add_to_cart > 0 or marketplace_checkout_flow > 0) as users_shop_intention,
    COUNT(DISTINCT amplitude_id) filter (where (shop_intention > 0 or marketplace_product_interaction > 0 or marketplace_product_add_to_cart > 0 or marketplace_checkout_flow > 0)
                                              and user_id IS NOT NULL) as users_logged_shop_intention, 
    COUNT(DISTINCT amplitude_id) filter (where (shop_intention > 0 or marketplace_product_interaction > 0 or marketplace_product_add_to_cart > 0 or marketplace_checkout_flow > 0)
                                              and user_id IS NULL) as users_guest_shop_intention,

    --Marketplace seen
    COUNT(DISTINCT amplitude_id) filter (where marketplace_seen > 0) as users_marketplace_seen,
    COUNT(DISTINCT amplitude_id) filter (where marketplace_seen > 0 and user_id IS NOT NULL) as users_logged_marketplace_seen,
    COUNT(DISTINCT amplitude_id) filter (where marketplace_seen > 0 and user_id IS NULL) as users_guest_marketplace_seen,

    --Marketplace browsing
    COUNT(DISTINCT amplitude_id) filter (where marketplace_browsing > 0) as users_marketplace_browsing,
    COUNT(DISTINCT amplitude_id) filter (where marketplace_browsing > 0 and user_id IS NOT NULL) as users_logged_marketplace_browsing,
    COUNT(DISTINCT amplitude_id) filter (where marketplace_browsing > 0 and user_id IS NULL) as users_guest_marketplace_browsing,
    
    --Product Interaction
    COUNT(DISTINCT amplitude_id) filter (where marketplace_product_interaction > 0) as users_product_interaction,
    COUNT(DISTINCT amplitude_id) filter (where marketplace_product_interaction > 0 and user_id IS NOT NULL) as users_logged_product_interaction,
    COUNT(DISTINCT amplitude_id) filter (where marketplace_product_interaction > 0 and user_id IS NULL) as users_guest_product_interaction,

    --Add to cart
    COUNT(DISTINCT amplitude_id) filter (where marketplace_product_add_to_cart > 0) as users_add_to_cart,
    COUNT(DISTINCT amplitude_id) filter (where marketplace_product_add_to_cart > 0 and user_id IS NOT NULL) as users_logged_add_to_cart,
    COUNT(DISTINCT amplitude_id) filter (where marketplace_product_add_to_cart > 0 and user_id IS NULL) as users_guest_add_to_cart,

    --Marketplace Checkout flow
    COUNT(DISTINCT amplitude_id) filter (where marketplace_checkout_flow > 0) as users_mktplace_checkout,
    COUNT(DISTINCT amplitude_id) filter (where marketplace_checkout_flow > 0 and user_id IS NOT NULL) as users_logged_mktplace_checkout,
    COUNT(DISTINCT amplitude_id) filter (where marketplace_checkout_flow > 0 and user_id IS NULL) as users_guest_mktplace_checkout,


    -- Store Clicked 
    COUNT(DISTINCT amplitude_id) filter (where  shop_intention>0 or deal_clicked>0) AS users_store_clicked,
    COUNT(DISTINCT amplitude_id) filter (where (shop_intention>0 or deal_clicked>0) and user_id IS NOT NULL) AS users_logged_store_clicked,
    COUNT(DISTINCT amplitude_id) filter (where (shop_intention>0 or deal_clicked>0) and user_id IS NULL) AS users_guest_store_clicked
  FROM {{ ref('agg_app_weekly_profile') }}
  WHERE 1=1
    AND year(sessions_date) >= 2023
  GROUP BY 1
),
weekly_transactions AS (
  SELECT
    DATE_TRUNC('WEEK', a.application_date_local) AS period,

    --Applications
    COUNT(DISTINCT a.application_id) FILTER (WHERE a.is_addishop_referral = TRUE or a.ally_slug = 'addi-marketplace') AS shop_applications,
    COUNT(DISTINCT a.application_id) FILTER (WHERE a.is_addishop_referral = TRUE) AS referral_applications,
    COUNT(DISTINCT a.application_id) FILTER (WHERE a.is_addishop_referral = TRUE AND a.is_addishop_referral_paid = TRUE) AS referral_paid_applications,
    COUNT(DISTINCT a.application_id) FILTER (WHERE a.ally_slug = 'addi-marketplace') AS mktplace_applications,
    
    --Clients Applying
    COUNT(DISTINCT a.client_id) FILTER (WHERE a.is_addishop_referral = TRUE or a.ally_slug = 'addi-marketplace') AS users_shop_applying,
    COUNT(DISTINCT a.client_id) FILTER (WHERE a.is_addishop_referral = TRUE) AS referral_users_applying,
    COUNT(DISTINCT a.client_id) FILTER (WHERE a.is_addishop_referral = TRUE AND a.is_addishop_referral_paid = TRUE) AS referral_paid_users_applying,
    COUNT(DISTINCT a.client_id) FILTER (WHERE a.ally_slug = 'addi-marketplace') AS mktplace_users_applying,
    
    --Originations
    COUNT(DISTINCT b.application_id) FILTER (WHERE a.is_addishop_referral = TRUE or a.ally_slug = 'addi-marketplace') as shop_originations,
    COUNT(DISTINCT b.application_id) FILTER (WHERE a.is_addishop_referral = TRUE) as referral_originations,
    COUNT(DISTINCT b.application_id) FILTER (WHERE a.is_addishop_referral = TRUE AND a.is_addishop_referral_paid = TRUE) as referral_paid_originations,
    COUNT(DISTINCT b.application_id) FILTER (WHERE a.ally_slug = 'addi-marketplace') as mktplace_originations,

    --Clients originating
    COUNT(DISTINCT b.client_id) FILTER (WHERE a.is_addishop_referral = TRUE or a.ally_slug = 'addi-marketplace') as users_shop_originating,
    COUNT(DISTINCT b.client_id) FILTER (WHERE a.is_addishop_referral = TRUE) as referral_users_originating,
    COUNT(DISTINCT b.client_id) FILTER (WHERE a.is_addishop_referral = TRUE AND a.is_addishop_referral_paid = TRUE) as referral_paid_users_originating,
    COUNT(DISTINCT b.client_id) FILTER (WHERE a.ally_slug = 'addi-marketplace') as mktplace_users_originating
  FROM  {{ ref('dm_applications') }} a
  LEFT JOIN  {{ ref('dm_originations') }} b
    ON a.application_id = b.application_id
  WHERE 1=1
    AND year(a.application_date_local) >= 2023
  GROUP BY 1
),
daily_app_profiles AS (
  SELECT 
    DATE_TRUNC('DAY', sessions_date) AS period,

    --App Users
    COUNT(DISTINCT amplitude_id) AS users_total,
    COUNT(DISTINCT amplitude_id) filter (where user_id IS NOT NULL) AS users_logged,
    COUNT(DISTINCT amplitude_id) filter (where user_id IS NULL) AS users_guest,

    --Shop Behavior
    COUNT(DISTINCT amplitude_id) filter (where profile IN ('Shop','Shop & Pay')) as users_shop_behavior, 
    COUNT(DISTINCT amplitude_id) filter (where user_id IS NOT NULL and profile IN ('Shop','Shop & Pay')) as users_logged_shop_behavior,
    COUNT(DISTINCT amplitude_id) filter (where user_id IS NULL and profile IN ('Shop','Shop & Pay')) as users_guest_shop_behavior,
    

    --Marketplace Behavior 
    COUNT(DISTINCT amplitude_id) filter (where  marketplace_browsing > 0 or marketplace_product_interaction > 0 or marketplace_product_add_to_cart >0 or marketplace_search >0 or marketplace_checkout_flow>0) as users_marketplace_behavior,
    COUNT(DISTINCT amplitude_id) filter (where (marketplace_browsing > 0 or marketplace_product_interaction > 0 or marketplace_product_add_to_cart >0 or marketplace_search >0 or marketplace_checkout_flow>0) and user_id IS NOT NULL ) as users_logged_marketplace_behavior,
    COUNT(DISTINCT amplitude_id) filter (where (marketplace_browsing > 0 or marketplace_product_interaction > 0 or marketplace_product_add_to_cart >0 or marketplace_search >0 or marketplace_checkout_flow>0) and user_id IS NULL ) as users_guest_marketplace_behavior,


    -- Referral Behavior 
    COUNT(DISTINCT amplitude_id) filter (where  shop_browsing > 0 or shop_search > 0 or shop_intention >0 or deal_curiosity >0 or deal_clicked>0) as users_referral_behavior,
    COUNT(DISTINCT amplitude_id) filter (where (shop_browsing > 0 or shop_search > 0 or shop_intention >0 or deal_curiosity >0 or deal_clicked>0) and user_id IS NOT NULL ) as users_logged_referral_behavior,
    COUNT(DISTINCT amplitude_id) filter (where (shop_browsing > 0 or shop_search > 0 or shop_intention >0 or deal_curiosity >0 or deal_clicked>0) and user_id IS NULL ) as users_guest_referral_behavior,

    
    --Shop Intention
    COUNT(DISTINCT amplitude_id) filter (where shop_intention > 0 or marketplace_product_interaction > 0 or marketplace_product_add_to_cart > 0 or marketplace_checkout_flow > 0) as users_shop_intention,
    COUNT(DISTINCT amplitude_id) filter (where (shop_intention > 0 or marketplace_product_interaction > 0 or marketplace_product_add_to_cart > 0 or marketplace_checkout_flow > 0)
                                              and user_id IS NOT NULL) as users_logged_shop_intention, 
    COUNT(DISTINCT amplitude_id) filter (where (shop_intention > 0 or marketplace_product_interaction > 0 or marketplace_product_add_to_cart > 0 or marketplace_checkout_flow > 0)
                                              and user_id IS NULL) as users_guest_shop_intention,

    --Marketplace seen
    COUNT(DISTINCT amplitude_id) filter (where marketplace_seen > 0) as users_marketplace_seen,
    COUNT(DISTINCT amplitude_id) filter (where marketplace_seen > 0 and user_id IS NOT NULL) as users_logged_marketplace_seen,
    COUNT(DISTINCT amplitude_id) filter (where marketplace_seen > 0 and user_id IS NULL) as users_guest_marketplace_seen,

    --Marketplace browsing
    COUNT(DISTINCT amplitude_id) filter (where marketplace_browsing > 0) as users_marketplace_browsing,
    COUNT(DISTINCT amplitude_id) filter (where marketplace_browsing > 0 and user_id IS NOT NULL) as users_logged_marketplace_browsing,
    COUNT(DISTINCT amplitude_id) filter (where marketplace_browsing > 0 and user_id IS NULL) as users_guest_marketplace_browsing,
    
    --Product Interaction
    COUNT(DISTINCT amplitude_id) filter (where marketplace_product_interaction > 0) as users_product_interaction,
    COUNT(DISTINCT amplitude_id) filter (where marketplace_product_interaction > 0 and user_id IS NOT NULL) as users_logged_product_interaction,
    COUNT(DISTINCT amplitude_id) filter (where marketplace_product_interaction > 0 and user_id IS NULL) as users_guest_product_interaction,

    --Add to cart
    COUNT(DISTINCT amplitude_id) filter (where marketplace_product_add_to_cart > 0) as users_add_to_cart,
    COUNT(DISTINCT amplitude_id) filter (where marketplace_product_add_to_cart > 0 and user_id IS NOT NULL) as users_logged_add_to_cart,
    COUNT(DISTINCT amplitude_id) filter (where marketplace_product_add_to_cart > 0 and user_id IS NULL) as users_guest_add_to_cart,

    --Marketplace Checkout flow
    COUNT(DISTINCT amplitude_id) filter (where marketplace_checkout_flow > 0) as users_mktplace_checkout,
    COUNT(DISTINCT amplitude_id) filter (where marketplace_checkout_flow > 0 and user_id IS NOT NULL) as users_logged_mktplace_checkout,
    COUNT(DISTINCT amplitude_id) filter (where marketplace_checkout_flow > 0 and user_id IS NULL) as users_guest_mktplace_checkout,


    -- Store Clicked 
    COUNT(DISTINCT amplitude_id) filter (where  shop_intention>0 or deal_clicked>0) AS users_store_clicked,
    COUNT(DISTINCT amplitude_id) filter (where (shop_intention>0 or deal_clicked>0) and user_id IS NOT NULL) AS users_logged_store_clicked,
    COUNT(DISTINCT amplitude_id) filter (where (shop_intention>0 or deal_clicked>0) and user_id IS NULL) AS users_guest_store_clicked
  FROM {{ ref('agg_app_daily_profile') }}
  WHERE 1=1
    AND year(sessions_date) >= 2023
  GROUP BY 1
),
daily_transactions AS (
  SELECT
    DATE_TRUNC('DAY', a.application_date_local) AS period,

    --Applications
    COUNT(DISTINCT a.application_id) FILTER (WHERE a.is_addishop_referral = TRUE or a.ally_slug = 'addi-marketplace') AS shop_applications,
    COUNT(DISTINCT a.application_id) FILTER (WHERE a.is_addishop_referral = TRUE) AS referral_applications,
    COUNT(DISTINCT a.application_id) FILTER (WHERE a.is_addishop_referral = TRUE AND a.is_addishop_referral_paid = TRUE) AS referral_paid_applications,
    COUNT(DISTINCT a.application_id) FILTER (WHERE a.ally_slug = 'addi-marketplace') AS mktplace_applications,
    --Clients Applying
    COUNT(DISTINCT a.client_id) FILTER (WHERE a.is_addishop_referral = TRUE or a.ally_slug = 'addi-marketplace') AS users_shop_applying,
    COUNT(DISTINCT a.client_id) FILTER (WHERE a.is_addishop_referral = TRUE) AS referral_users_applying,                        
    COUNT(DISTINCT a.client_id) FILTER (WHERE a.is_addishop_referral = TRUE AND a.is_addishop_referral_paid = TRUE) AS referral_paid_users_applying,
    COUNT(DISTINCT a.client_id) FILTER (WHERE a.ally_slug = 'addi-marketplace') AS mktplace_users_applying,
    
    --Originations
    COUNT(DISTINCT b.application_id) FILTER (WHERE a.is_addishop_referral = TRUE or a.ally_slug = 'addi-marketplace') as shop_originations,
    COUNT(DISTINCT b.application_id) FILTER (WHERE a.is_addishop_referral = TRUE) as referral_originations,
    COUNT(DISTINCT b.application_id) FILTER (WHERE a.is_addishop_referral = TRUE AND a.is_addishop_referral_paid = TRUE) as referral_paid_originations,
    COUNT(DISTINCT b.application_id) FILTER (WHERE a.ally_slug = 'addi-marketplace') as mktplace_originations,

    --Clients originating
    COUNT(DISTINCT b.client_id) FILTER (WHERE a.is_addishop_referral = TRUE or a.ally_slug = 'addi-marketplace') as users_shop_originating,
    COUNT(DISTINCT b.client_id) FILTER (WHERE a.is_addishop_referral = TRUE) as referral_users_originating,
    COUNT(DISTINCT b.client_id) FILTER (WHERE a.is_addishop_referral = TRUE AND a.is_addishop_referral_paid = TRUE) as referral_paid_users_originating,
    COUNT(DISTINCT b.client_id) FILTER (WHERE a.ally_slug = 'addi-marketplace') as mktplace_users_originating
  FROM  {{ ref('dm_applications') }} a
  LEFT JOIN  {{ ref('dm_originations') }} b
    ON a.application_id = b.application_id
  WHERE 1=1
    AND year(a.application_date_local) >= 2023
  GROUP BY 1
)
SELECT
  'MONTHLY' AS period_type,
  a.period,
  a.users_total,
  a.users_logged,
  a.users_guest,

  a.users_shop_behavior,
  a.users_logged_shop_behavior,
  a.users_guest_shop_behavior,

  a.users_marketplace_behavior,
  a.users_logged_marketplace_behavior,
  a.users_guest_marketplace_behavior,

  a.users_referral_behavior,
  a.users_logged_referral_behavior,
  a.users_guest_referral_behavior,

  a.users_shop_intention,
  a.users_logged_shop_intention,
  a.users_guest_shop_intention,

  a.users_marketplace_seen,
  a.users_logged_marketplace_seen,
  a.users_guest_marketplace_seen,

  a.users_marketplace_browsing,
  a.users_logged_marketplace_browsing,
  a.users_guest_marketplace_browsing,

  a.users_product_interaction,
  a.users_logged_product_interaction,
  a.users_guest_product_interaction,
  
  a.users_add_to_cart,
  a.users_logged_add_to_cart,
  a.users_guest_add_to_cart,

  a.users_mktplace_checkout,
  a.users_logged_mktplace_checkout,
  a.users_guest_mktplace_checkout,

  a.users_store_clicked,
  a.users_logged_store_clicked,
  a.users_guest_store_clicked,

  b.shop_applications,
  b.referral_applications,
  b.referral_paid_applications,
  b.mktplace_applications,

  b.users_shop_applying,
  b.referral_users_applying,
  b.referral_paid_users_applying,
  b.mktplace_users_applying,
  
  b.shop_originations,
  b.referral_originations,
  b.referral_paid_originations,
  b.mktplace_originations,
  
  b.users_shop_originating,
  b.referral_users_originating,
  b.referral_paid_users_originating,
  b.mktplace_users_originating
FROM monthly_app_profiles a
LEFT JOIN monthly_transactions b ON a.period = b.period

UNION ALL

SELECT
  'WEEKLY' AS period_type,
  a.period,
  a.users_total,
  a.users_logged,
  a.users_guest,

  a.users_shop_behavior,
  a.users_logged_shop_behavior,
  a.users_guest_shop_behavior,

  a.users_marketplace_behavior,
  a.users_logged_marketplace_behavior,
  a.users_guest_marketplace_behavior,

  a.users_referral_behavior,
  a.users_logged_referral_behavior,
  a.users_guest_referral_behavior,

  a.users_shop_intention,
  a.users_logged_shop_intention,
  a.users_guest_shop_intention,

  a.users_marketplace_seen,
  a.users_logged_marketplace_seen,
  a.users_guest_marketplace_seen,

  a.users_marketplace_browsing,
  a.users_logged_marketplace_browsing,
  a.users_guest_marketplace_browsing,

  a.users_product_interaction,
  a.users_logged_product_interaction,
  a.users_guest_product_interaction,
  
  a.users_add_to_cart,
  a.users_logged_add_to_cart,
  a.users_guest_add_to_cart,

  a.users_mktplace_checkout,
  a.users_logged_mktplace_checkout,
  a.users_guest_mktplace_checkout,

  a.users_store_clicked,
  a.users_logged_store_clicked,
  a.users_guest_store_clicked,

  b.shop_applications,
  b.referral_applications,
  b.referral_paid_applications,
  b.mktplace_applications,

  b.users_shop_applying,
  b.referral_users_applying,
  b.referral_paid_users_applying,
  b.mktplace_users_applying,
  
  b.shop_originations,
  b.referral_originations,
  b.referral_paid_originations,
  b.mktplace_originations,
  
  b.users_shop_originating,
  b.referral_users_originating,
  b.referral_paid_users_originating,
  b.mktplace_users_originating
FROM weekly_app_profiles a
LEFT JOIN weekly_transactions b ON a.period = b.period

UNION ALL

SELECT
  'DAILY' AS period_type,
  a.period,
  a.users_total,
  a.users_logged,
  a.users_guest,

  a.users_shop_behavior,
  a.users_logged_shop_behavior,
  a.users_guest_shop_behavior,

  a.users_marketplace_behavior,
  a.users_logged_marketplace_behavior,
  a.users_guest_marketplace_behavior,

  a.users_referral_behavior,
  a.users_logged_referral_behavior,
  a.users_guest_referral_behavior,

  a.users_shop_intention,
  a.users_logged_shop_intention,
  a.users_guest_shop_intention,

  a.users_marketplace_seen,
  a.users_logged_marketplace_seen,
  a.users_guest_marketplace_seen,

  a.users_marketplace_browsing,
  a.users_logged_marketplace_browsing,
  a.users_guest_marketplace_browsing,

  a.users_product_interaction,
  a.users_logged_product_interaction,
  a.users_guest_product_interaction,
  
  a.users_add_to_cart,
  a.users_logged_add_to_cart,
  a.users_guest_add_to_cart,

  a.users_mktplace_checkout,
  a.users_logged_mktplace_checkout,
  a.users_guest_mktplace_checkout,

  a.users_store_clicked,
  a.users_logged_store_clicked,
  a.users_guest_store_clicked,

  b.shop_applications,
  b.referral_applications,
  b.referral_paid_applications,
  b.mktplace_applications,

  b.users_shop_applying,
  b.referral_users_applying,
  b.referral_paid_users_applying,
  b.mktplace_users_applying,
  
  b.shop_originations,
  b.referral_originations,
  b.referral_paid_originations,
  b.mktplace_originations,
  
  b.users_shop_originating,
  b.referral_users_originating,
  b.referral_paid_users_originating,
  b.mktplace_users_originating
FROM daily_app_profiles a
LEFT JOIN daily_transactions b ON a.period = b.period
