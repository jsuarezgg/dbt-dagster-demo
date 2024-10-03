{{
    config(
        materialized='incremental',
        unique_key="surrogate_key",
        incremental_strategy='merge',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

-- QUERY
  SELECT
    --Sometimes we have a client attempting to use as a prospect and then logins with user_id in the same session like session_id 1712782973189
    --In order to avoid that we use a MAX to keep the record with the user_id
    {{ dbt_utils.surrogate_key(['amplitude_id', 'session_id','MIN(event_time_local)']) }} AS surrogate_key,
    MAX(user_id) AS user_id,
    amplitude_id,
    --AGREGAR USER PROPERTIES!
    MAX(COALESCE(user_properties:['role'] ,
                event_properties:['role'] )) AS role,

    MAX(COALESCE(user_properties:['addiCupoStateV2'] ,
                event_properties:['addiCupo.stateV2'] )) AS cupo_state_v2,

    MAX(COALESCE(user_properties:['addiCupoTotal'] ,
                event_properties:['addiCupo.totalAddiCupo'] )) AS addiCupoTotal,
    
    --Min because sometimes the client is paying the debt in the session and this value is updated
    MIN(COALESCE(user_properties:['addiCupoUsed'] ,
                event_properties:['addiCupo.usedAddiCupo'] )) AS addiCupoUsed,
    MAX(COALESCE(user_properties:['addiCupoBalance'] ,
                event_properties:['addiCupo.balanceAddiCupo'] )) AS addiCupoBalance,
    MAX(COALESCE(user_properties:['isTransactionalBased'] ,
                event_properties:['isTransactionalBased'] )) AS isTransactionalBased,
    MAX(COALESCE(user_properties:['grande'] ,
                event_properties:['grande'] )) AS grande,
    MAX(COALESCE(user_properties:['preapprovalExpirationDate'] ,
                event_properties:['preapproval.expirationDate'] )) AS preapprovalExpirationDate,
    MAX(COALESCE(user_properties:['preapprovalAmount'] ,
                event_properties:['preapproval.amount'] )) AS preapprovalAmount,
    MAX(COALESCE(user_properties:['paymentStatus'] ,
                event_properties:['paymentStatus'] )) AS paymentStatus,
    MAX(COALESCE(user_properties:['daysPastDue'] ,
                event_properties:['daysPastDue'] )) AS daysPastDue,
    MAX(user_properties:['toggle.isMarketplaceTheMainScreen'] ) AS marketplace_default_screen,
    session_id,
    MIN(platform) AS platform,
    MAX(version_name) AS version_name,
    MAX(city) AS city,
    MIN(event_time_local) AS session_start_time,
    MAX(event_time_local) AS session_end_time,
    datediff(
      SECOND,
      MIN(event_time_local),
      MAX(event_time_local)
    ) AS session_duration_secs,
    COUNT(*) AS total_events,
    
    COLLECT_SET(
      named_struct(
        'event_time_local',
        event_time_local,
        'event_type',
        event_type,
        'screenName',
        cast(
          get_json_object(event_properties, '$.screenName') AS string
        ),
        'selectedTab',
        cast(
          get_json_object(event_properties, '$.selectedTab') AS string
        )
      )
    ) AS total_events_list,

    --Events categorization

    --Shop Home seen
  COUNT(*) FILTER (WHERE event_type = 'APP_SCREEN_OPENED' AND cast(get_json_object(event_properties, '$.screenName') AS string) = 'HOME') AS shop_home_seen,

     -- Shop browsing
    COUNT(*) FILTER (
      WHERE
        event_type IN (
          'SELECT_SUBCATEGORY_TAG',
          'SHOP_CATEGORY_TAPPED',
          'HOME_COMPRA_GRANDE_BANNER_TAPPED',
          'HOME_FEATURED_SECTION_SEE_ALL_TAPPED',
          'HOME_COMPRA_GRANDE_SELECT_MORE_INFO',
          'SELECT_REWARDS_MENU',
          'SELECT_EXPAND_SUBCATEGORY',
          'HOME_SEE_ALL_STORES_BUTTON_TAPPED',
          'HOME_PROMOTED_BANNER_TAPPED',
          'APP_SELECT_EXPLORE_STORES',
          'SELECT_REWARDS_FILTER',
          'SELECT_SEE_ALL_REWARDS_STORES'
        )
        OR (
          event_type = 'APP_SCREEN_OPENED'
          AND cast(
            get_json_object(event_properties, '$.screenName') AS string
          ) IN (
            'SHOP_NOW',
            'STORES',
            'CATEGORY',
            'SUBCATEGORY',
            'CATEGORIES',
            'PREFERRED',
            'REWARDS_MENU',
            'HOW_TO_BUY_MODAL',
            'COMPRA_GRANDE',
            'PSE_INFO'
          )
        )
        OR (
          event_type = 'APP_NAVBAR_BUTTON_TAPPED'
          AND cast(
            get_json_object(event_properties, '$.selectedTab') AS string
          ) IN ('SHOP_NOW','STORES')
    )) AS shop_browsing,

    COLLECT_SET(
      named_struct(
        'event_type',
        event_type,
        'screenName',
        cast(
          get_json_object(event_properties, '$.screenName') AS string
        )
      )
    ) FILTER (
      WHERE
        event_type IN (
          'SELECT_SUBCATEGORY_TAG',
          'SHOP_CATEGORY_TAPPED',
          'HOME_COMPRA_GRANDE_BANNER_TAPPED',
          'HOME_FEATURED_SECTION_SEE_ALL_TAPPED',
          'HOME_COMPRA_GRANDE_SELECT_MORE_INFO',
          'SELECT_REWARDS_MENU',
          'SELECT_EXPAND_SUBCATEGORY',
          'HOME_SEE_ALL_STORES_BUTTON_TAPPED',
          'HOME_PROMOTED_BANNER_TAPPED',
          'APP_SELECT_EXPLORE_STORES',
          'SELECT_REWARDS_FILTER',
          'SELECT_SEE_ALL_REWARDS_STORES'
        )
        OR (
          event_type = 'APP_SCREEN_OPENED'
          AND cast(
            get_json_object(event_properties, '$.screenName') AS string
          ) IN (
            'SHOP_NOW',
            'STORES',
            'CATEGORY',
            'SUBCATEGORY',
            'CATEGORIES',
            'PREFERRED',
            'REWARDS_MENU',
            'HOW_TO_BUY_MODAL',
            'COMPRA_GRANDE',
            'PSE_INFO'
          )
        )
        OR (
          event_type = 'APP_NAVBAR_BUTTON_TAPPED'
          AND cast(
            get_json_object(event_properties, '$.selectedTab') AS string
          ) IN ('SHOP_NOW','STORES')
        )

    ) AS shop_browsing_debug,
    
    -- Shop Search
    COUNT(*) FILTER (
      WHERE
        event_type = 'SHOP_SEARCH_BAR_TAPPED' --AND event_properties:['searchedWord'] IS NOT NULL
    ) AS shop_search,
    COLLECT_SET(
      named_struct(
        'event_type',
        event_type,
        'searchedWord',
        cast(
          get_json_object(event_properties, '$.searchedWord') AS string
        )
      )
    ) FILTER (
      WHERE
        event_type = 'SHOP_SEARCH_BAR_TAPPED' --AND event_properties:['searchedWord'] IS NOT NULL
    ) AS shop_search_debug,


    --Shop intent/click
    COUNT(*) FILTER (
      WHERE
        event_type IN (
          'SELECT_STORE',
          'SELECT_GO_TO_WEBSITE',
          'HOME_STORE_TAPPED',
          'SHOP_STORE_TAPPED'
        )
        OR (
          event_type = 'APP_SCREEN_OPENED'
          AND cast(
            get_json_object(event_properties, '$.screenName') AS string
          ) IN ('MERCHANT_PROFILE', 'REDIRECT_STORE')
        )
    ) AS shop_intention,
    COLLECT_SET(
      named_struct(
        'event_type',
        event_type,
        'screenName',
        cast(
          get_json_object(event_properties, '$.screenName') AS string
        )
      )
    ) FILTER (
      WHERE
        event_type IN (
          'SELECT_STORE',
          'SELECT_GO_TO_WEBSITE',
          'HOME_STORE_TAPPED',
          'SHOP_STORE_TAPPED'
        )
        OR (
          event_type = 'APP_SCREEN_OPENED'
          AND cast(
            get_json_object(event_properties, '$.screenName') AS string
          ) IN ('MERCHANT_PROFILE', 'REDIRECT_STORE')
        )
    ) AS shop_intention_debug,
   
    --Deals curiosity
    COUNT(*) FILTER (
      WHERE
        (
          event_type = 'APP_SCREEN_OPENED'
          AND cast(
            get_json_object(event_properties, '$.screenName') AS string
          ) = 'DEALS'
        )
        OR (
          event_type = 'APP_NAVBAR_BUTTON_TAPPED'
          AND cast(
            get_json_object(event_properties, '$.selectedTab') AS string
          ) = 'DEALS'
        )
        OR (
          event_type IN (
            'APP_SWIPE_DEAL',
            'APP_SELECT_DEALS_BANNER',
            'APP_SELECT_SAVED_DEALS'
          )
        )
    ) AS deal_curiosity,
    COLLECT_SET(
      named_struct(
        'event_type',
        event_type,
        'screenName',
        cast(
          get_json_object(event_properties, '$.screenName') AS string
        ),
        'selectedTab',
        cast(
          get_json_object(event_properties, '$.selectedTab') AS string
        )
      )
    ) FILTER (
      WHERE
        (
          event_type = 'APP_SCREEN_OPENED'
          AND cast(
            get_json_object(event_properties, '$.screenName') AS string
          ) = 'DEALS'
        )
        OR (
          event_type = 'APP_NAVBAR_BUTTON_TAPPED'
          AND cast(
            get_json_object(event_properties, '$.selectedTab') AS string
          ) = 'DEALS'
        )
        OR (
          event_type IN (
            'APP_SWIPE_DEAL',
            'APP_SELECT_DEALS_BANNER',
            'APP_SELECT_SAVED_DEALS'
          )
        )
    ) AS deal_curiosity_debug,
    
    --Deals click
    COUNT(*) FILTER (
      WHERE
        event_type = 'SELECT_DEAL'
    ) AS deal_clicked,
    COLLECT_SET(
      cast(
        get_json_object(event_properties, '$.allySlug') AS string
      )
    ) FILTER (
      WHERE
        event_type = 'SELECT_DEAL'
    ) AS deal_clicked_debug,
    
   --Marketplace seen
COUNT(*) FILTER (WHERE event_type = 'VIEW_MARKETPLACE_SCREEN'

                OR (
          event_type = 'APP_NAVBAR_BUTTON_TAPPED'
          AND cast(
            get_json_object(event_properties, '$.selectedTab') AS string
          ) = 'MARKETPLACE'
        )
)
 AS marketplace_seen,

    COLLECT_SET(
      named_struct(
        'event_time_local',
        event_time_local,
        'event_type',
        event_type,
        'screenName',
        cast(
          get_json_object(event_properties, '$.screenName') AS string
        ),
        'selectedTab',
        cast(
          get_json_object(event_properties, '$.selectedTab') AS string
        )
      )
    )

FILTER (WHERE event_type = 'VIEW_MARKETPLACE_SCREEN'

                OR (
          event_type = 'APP_NAVBAR_BUTTON_TAPPED'
          AND cast(
            get_json_object(event_properties, '$.selectedTab') AS string
          ) = 'MARKETPLACE'
        )
) AS marketplace_seen_debug,
    
    -- Marketplace browsing
    COUNT(*) FILTER (
      WHERE
        event_type IN (
          'SELECT_MARKETPLACE_DEPARTMENT',
          'SELECT_MARKETPLACE_MENU',
          'SELECT_MARKETPLACE_SELLER'
        )
        OR (
          event_type = 'VIEW_MARKETPLACE_SCREEN'
          AND cast(
            get_json_object(event_properties, '$.screenName') AS string
          ) IN (
            'MARKETPLACE_DEPARTMENT',
            'MARKETPLACE_SELLER',
            'MARKETPLACE_CATEGORY',
            'MARKETPLACE_SUBCATEGORY',
            'MARKETPLACE_BRAND',
            'MARKETPLACE_MENU'
          )
        )
         OR (event_type = 'SELECT_MARKETPLACE_BANNER' --AND cast(get_json_object(event_properties, '$.screenName') AS string) NOT IN ('HOME','MERCHANT_PROFILE')
         )

    ) AS marketplace_browsing,
    COLLECT_SET(
      named_struct(
        'event_type',
        event_type,
        'screenName',
        cast(
          get_json_object(event_properties, '$.screenName') AS string
        ),
        'selectedTab',
        cast(
          get_json_object(event_properties, '$.selectedTab') AS string
        )
      )
    ) FILTER (
      WHERE
        event_type IN (
          'SELECT_MARKETPLACE_DEPARTMENT',
          'SELECT_MARKETPLACE_MENU',
          'SELECT_MARKETPLACE_SELLER'
        )
        OR (
          event_type = 'VIEW_MARKETPLACE_SCREEN'
          AND cast(
            get_json_object(event_properties, '$.screenName') AS string
          ) IN (
            'MARKETPLACE_DEPARTMENT',
            'MARKETPLACE_SELLER',
            'MARKETPLACE_CATEGORY',
            'MARKETPLACE_SUBCATEGORY',
            'MARKETPLACE_BRAND',
            'MARKETPLACE_MENU'
          )
          
        )
         OR (event_type = 'SELECT_MARKETPLACE_BANNER' --AND cast(get_json_object(event_properties, '$.screenName') AS string) NOT IN ('HOME','MERCHANT_PROFILE')
         )
    ) AS marketplace_browsing_debug,
    
     -- Marketplace search
    COUNT(*) FILTER (
      WHERE
        event_type = 'SELECT_TOP_SEARCHED_KEYWORD'
        OR (
          event_type = 'VIEW_MARKETPLACE_SCREEN'
          AND cast(
            get_json_object(event_properties, '$.screenName') AS string
          ) IN (
            'MARKETPLACE_SEARCH_RELATED_SUGGESTION',
            'MARKETPLACE_SEARCH',
            'MARKETPLACE_SEARCH_RESULTS'
          )
        )
    ) AS marketplace_search,
    COLLECT_SET(
      named_struct(
        'event_type',
        event_type,
        'screenName',
        cast(
          get_json_object(event_properties, '$.screenName') AS string
        ),
        'searchedWord',
        cast(
          get_json_object(event_properties, '$.searchedWord') AS string
        )
      )
    ) FILTER (
      WHERE
        event_type = 'SELECT_TOP_SEARCHED_KEYWORD'
        OR (
          event_type = 'VIEW_MARKETPLACE_SCREEN'
          AND cast(
            get_json_object(event_properties, '$.screenName') AS string
          ) IN (
            'MARKETPLACE_SEARCH_RELATED_SUGGESTION',
            'MARKETPLACE_SEARCH',
            'MARKETPLACE_SEARCH_RESULTS'
          )
        )
    ) AS marketplace_search_debug,
    
    
    -- Marketplace product interaction
    COUNT(*) FILTER (
      WHERE
        event_type IN (
          'SELECT_MARKETPLACE_PRODUCT',
          'SELECT_ADD_TO_CART',
          'SELECT_QUICK_ADD_MARKETPLACE_PRODUCT',
          'SELECT_FAV_MARKETPLACE_PRODUCT'
        )
        OR (
          event_type = 'VIEW_MARKETPLACE_SCREEN'
          AND cast(
            get_json_object(event_properties, '$.screenName') AS string
          ) = 'MARKETPLACE_PRODUCT_DETAIL'
        )

    ) AS marketplace_product_interaction,
    COLLECT_SET(
      named_struct(
        'event_type',
        event_type,
        'screenName',
        cast(
          get_json_object(event_properties, '$.screenName') AS string
        )
      )
    ) FILTER (
      WHERE
        event_type IN (
          'SELECT_MARKETPLACE_PRODUCT',
          'SELECT_ADD_TO_CART',
          'SELECT_QUICK_ADD_MARKETPLACE_PRODUCT',
          'SELECT_FAV_MARKETPLACE_PRODUCT'
        )
        OR (
          event_type = 'VIEW_MARKETPLACE_SCREEN'
          AND cast(
            get_json_object(event_properties, '$.screenName') AS string
          ) = 'MARKETPLACE_PRODUCT_DETAIL'
        )
    ) AS marketplace_product_interaction_debug,

---- Marketplace product add to cart
    COUNT(*) FILTER (
      WHERE
        event_type =
          'SELECT_ADD_TO_CART'

    ) AS marketplace_product_add_to_cart,

    COLLECT_SET(
      named_struct(
        'event_type',
        event_type,
        'productName',
        cast(
          get_json_object(event_properties, '$.productName') AS string
        )
      )
    ) FILTER (
      WHERE
        event_type = 'SELECT_ADD_TO_CART') AS marketplace_product_add_to_cart_debug,
    
    -- Marketplace checkout flow
    COUNT(*) FILTER (
      WHERE
        event_type IN (
          'SELECT_COMPLETE_PURCHASE',
          'SELECT_CONTINUE_SHIPPING_INFORMATION',
          'SELECT_CONTINUE_MARKETPLACE_PAYMENT',
          'SELECT_CART'
        )
        OR (
          event_type = 'VIEW_MARKETPLACE_SCREEN'
          AND cast(
            get_json_object(event_properties, '$.screenName') AS string
          ) IN (
            'MARKETPLACE_CART_SUMMARY',
            'MARKETPLACE_SHIPPING_INFORMATION',
            'MARKETPLACE_PAYMENT_INFORMATION',
            'MARKETPLACE_ORDER_INFORMATION'
          )
        )
    ) AS marketplace_checkout_flow,
    COLLECT_SET(
      named_struct(
        'event_type',
        event_type,
        'screenName',
        cast(
          get_json_object(event_properties, '$.screenName') AS string
        )
      )
    ) FILTER (
      WHERE
        event_type IN (
          'SELECT_COMPLETE_PURCHASE',
          'SELECT_CONTINUE_SHIPPING_INFORMATION',
          'SELECT_CONTINUE_MARKETPLACE_PAYMENT',
          'SELECT_CART'
        )
        OR (
          event_type = 'VIEW_MARKETPLACE_SCREEN'
          AND cast(
            get_json_object(event_properties, '$.screenName') AS string
          ) IN (
            'MARKETPLACE_CART_SUMMARY',
            'MARKETPLACE_SHIPPING_INFORMATION',
            'MARKETPLACE_PAYMENT_INFORMATION',
            'MARKETPLACE_ORDER_INFORMATION'
          )
        )
    ) AS marketplace_checkout_flow_debug,
   
    --Marketplace purchase feedback/confirmation
    COUNT(*) FILTER (
      WHERE
        event_type = 'VIEW_MARKETPLACE_SCREEN'
        AND cast(
          get_json_object(event_properties, '$.screenName') AS string
        ) = 'MARKETPLACE_PURCHASE_CONFIRMED'
    ) AS marketplace_purchase_confirmed,
    COLLECT_SET(
      named_struct(
        'event_type',
        event_type,
        'screenName',
        cast(
          get_json_object(event_properties, '$.screenName') AS string
        )
      )
    ) FILTER (
      WHERE
        event_type = 'VIEW_MARKETPLACE_SCREEN'
        AND cast(
          get_json_object(event_properties, '$.screenName') AS string
        ) = 'MARKETPLACE_PURCHASE_CONFIRMED'
    ) AS marketplace_purchase_confirmed_debug,

--Nequi configuration
    COUNT(*) FILTER (
      WHERE
        event_type IN (
          'SELECT_CONFIRM_NEQUI_ACCOUNT_SUBSCRIPTION',
          'SELECT_NEQUI_SETTINGS',
          'SELECT_NOT_MY_NEQUI_PHONE_NUMBER',
          'SELECT_CONFIRM_UNSUBSCRIBE_NEQUI'
        )
        OR (
          event_type = 'APP_SCREEN_OPENED'
          AND cast(
            get_json_object(event_properties, '$.screenName') AS string
          ) IN (
            'NEQUI_SUBSCRIPTION_ACCEPTANCE',
            'NEQUI_SUBSCRIPTION_WELCOME',
            'NEQUI_SUBSCRIPTION_WAITING',
            'NEQUI_SUBSCRIPTION_SUCCESS',
            'NEQUI_SUBSCRIPTION_ERROR',
            'NEQUI_SUBSCRIPTION_INFORMATION',
            'NEQUI_UNSUBSCRIBE_SUCCESS',
            'NEQUI_UNSUBSCRIBE_ERROR'
          )
        )
    ) AS nequi_configuration,
    COLLECT_SET(
      named_struct(
        'event_type',
        event_type,
        'screenName',
        cast(
          get_json_object(event_properties, '$.screenName') AS string
        )
      )
    ) FILTER (
      WHERE
        event_type IN (
          'SELECT_CONFIRM_NEQUI_ACCOUNT_SUBSCRIPTION',
          'SELECT_NEQUI_SETTINGS',
          'SELECT_NOT_MY_NEQUI_PHONE_NUMBER',
          'SELECT_CONFIRM_UNSUBSCRIBE_NEQUI'
        )
        OR (
          event_type = 'APP_SCREEN_OPENED'
          AND cast(
            get_json_object(event_properties, '$.screenName') AS string
          ) IN (
            'NEQUI_SUBSCRIPTION_ACCEPTANCE',
            'NEQUI_SUBSCRIPTION_WELCOME',
            'NEQUI_SUBSCRIPTION_WAITING',
            'NEQUI_SUBSCRIPTION_SUCCESS',
            'NEQUI_SUBSCRIPTION_ERROR',
            'NEQUI_SUBSCRIPTION_INFORMATION',
            'NEQUI_UNSUBSCRIBE_SUCCESS',
            'NEQUI_UNSUBSCRIBE_ERROR'
          )
        )
    ) AS nequi_configuration_debug,

    --Pay curiosity
    COUNT(*) FILTER (
      WHERE
        event_type IN (
          'PAYMENTS_PAY_BUTTON_TAPPED',
          'PAYMENTS_HISTORY_TAPPED'
        )
        OR (
          event_type = 'PAYMENTS_TAB_SELECTED'
          AND UPPER(cast(
            get_json_object(event_properties, '$.tabName') AS string
          )) = 'PAYMENTS'
        )
        OR (event_type = 'APP_SCREEN_OPENED' AND UPPER(cast(
            get_json_object(event_properties, '$.screenName') AS string
          )) = 'PURCHASES')
   OR (event_type = 'APP_NAVBAR_BUTTON_TAPPED' AND UPPER(cast(
            get_json_object(event_properties, '$.selectedTab') AS string
          )) = 'PURCHASES')


    ) AS pay_curiosity,

    COLLECT_SET(
      named_struct(
        'event_type',
        event_type,
        'selectedTab',
        cast(
          get_json_object(event_properties, '$.tabName') AS string
        )
      )
    ) FILTER (
      WHERE
        event_type IN (
          'PAYMENTS_PAY_BUTTON_TAPPED',
          'PAYMENTS_HISTORY_TAPPED'
        )
        OR (
          event_type = 'PAYMENTS_TAB_SELECTED'
          AND upper(cast(
            get_json_object(event_properties, '$.tabName') AS string
          )) = 'PAYMENTS'
        )
                OR (event_type = 'APP_SCREEN_OPENED' AND UPPER(cast(
            get_json_object(event_properties, '$.screenName') AS string
          )) = 'PURCHASES')

             OR (event_type = 'APP_NAVBAR_BUTTON_TAPPED' AND UPPER(cast(
            get_json_object(event_properties, '$.selectedTab') AS string
          )) = 'PURCHASES')
    ) AS pay_curiosity_debug,
    
   
    --Pay intention
    COUNT(*) FILTER (
      WHERE
        event_type IN (
          'PAYMENTS_CO_CONTINUE_BUTTON_TAPPED',
          'PAYMENTS_CO_METHOD_SELECTED',
          'PAYMENTS_CO_AMOUNT_TO_PAY_SELECTED',
          'PAYMENTS_CO_PSE_CONTINUE_BUTTON_TAPPED',
          'PAYMENTS_CO_OTHER_AMOUNT_MODAL_OPENED',
          'SELECT_NEQUI_PAYMENT_CONFIRMATION',
          'PAYMENTS_CO_CORRESPONSAL_HOME_SCREEN_TAPPED',
          'PAYMENTS_CO_USE_ANOTHER_METHOD_TAPPED',
          'PAYMENTS_CO_CORRESPONSAL_DOWNLOAD_RECEIPT_TAPPED',
          'SELECT_CONTINUE_NEQUI_PAYMENT',
          'SELECT_USE_OTHER_PAYMENT_METHOD'
        )
        OR (
          event_type = 'APP_SCREEN_OPENED'
          AND cast(
            get_json_object(event_properties, '$.screenName') AS string
          ) IN (
            'PAYMENT_AMOUNT',
            'PAYMENT_METHOD',
            'CORRESPONSAL_BANCARIO',
            'CORRESPONSAL_PIN_SAVED',
            'NEQUI_TRANSACTION_CONFIRMATION',
            'PSE'
          )
        )
    ) AS pay_intention,
    COLLECT_SET(
      named_struct(
        'event_type',
        event_type,
        'screenName',
        cast(
          get_json_object(event_properties, '$.screenName') AS string
        )
      )
    ) FILTER (
      WHERE
        event_type IN (
          'PAYMENTS_CO_CONTINUE_BUTTON_TAPPED',
          'PAYMENTS_CO_METHOD_SELECTED',
          'PAYMENTS_CO_AMOUNT_TO_PAY_SELECTED',
          'PAYMENTS_CO_PSE_CONTINUE_BUTTON_TAPPED',
          'PAYMENTS_CO_OTHER_AMOUNT_MODAL_OPENED',
          'SELECT_NEQUI_PAYMENT_CONFIRMATION',
          'PAYMENTS_CO_CORRESPONSAL_HOME_SCREEN_TAPPED',
          'PAYMENTS_CO_USE_ANOTHER_METHOD_TAPPED',
          'PAYMENTS_CO_CORRESPONSAL_DOWNLOAD_RECEIPT_TAPPED',
          'SELECT_CONTINUE_NEQUI_PAYMENT',
          'SELECT_USE_OTHER_PAYMENT_METHOD'
        )
        OR (
          event_type = 'APP_SCREEN_OPENED'
          AND cast(
            get_json_object(event_properties, '$.screenName') AS string
          ) IN (
            'PAYMENT_AMOUNT',
            'PAYMENT_METHOD',
            'CORRESPONSAL_BANCARIO',
            'CORRESPONSAL_PIN_SAVED',
            'NEQUI_TRANSACTION_CONFIRMATION',
            'PSE'
          )
        )
    ) AS pay_intention_debug,
    
    --Pay feedback
    COUNT(*) FILTER (
      WHERE
        event_type IN (
          'PAYMENTS_CO_TRANSACTION_FAILED',
          'SELECT_FINISH_PURCHASE',
          'PAYMENTS_CO_TRANSACTION_SUCCESS'
        )
        OR (
          event_type = 'APP_SCREEN_OPENED'
          AND cast(
            get_json_object(event_properties, '$.screenName') AS string
          ) = 'PAYMENT_FEEDBACK'
        )
    ) AS pay_feedback,
    COLLECT_SET(
      named_struct(
        'event_type',
        event_type,
        'screenName',
        cast(
          get_json_object(event_properties, '$.screenName') AS string
        )
      )
    ) FILTER (
      WHERE
        event_type IN (
          'PAYMENTS_CO_TRANSACTION_FAILED',
          'SELECT_FINISH_PURCHASE',
          'PAYMENTS_CO_TRANSACTION_SUCCESS'
        )
        OR (
          event_type = 'APP_SCREEN_OPENED'
          AND cast(
            get_json_object(event_properties, '$.screenName') AS string
          ) = 'PAYMENT_FEEDBACK'
        )
    ) AS pay_feedback_debug,
    
    --Purchases curiosity
    COUNT(*) FILTER (
      WHERE
        event_type = 'PAYMENTS_PURCHASE_HISTORY_TAPPED'
                      
        OR (
          event_type = 'APP_SCREEN_OPENED'
          AND cast(
            get_json_object(event_properties, '$.screenName') AS string
          ) = 'PURCHASES'
        )

        OR (
          event_type = 'PAYMENTS_TAB_SELECTED'
          AND UPPER(cast(
            get_json_object(event_properties, '$.tabName') AS string
          )) = 'PURCHASES'
        )
    ) AS purchases_curiosity,
    COLLECT_SET(
      named_struct(
        'event_type',
        event_type,
        'screenName',
        cast(
          get_json_object(event_properties, '$.screenName') AS string
        ),
        'selectedTab',
        cast(
          get_json_object(event_properties, '$.selectedTab') AS string
        )
      )
    ) FILTER (
      WHERE
        event_type = 'PAYMENTS_PURCHASE_HISTORY_TAPPED'
        OR (
          event_type = 'APP_SCREEN_OPENED'
          AND cast(
            get_json_object(event_properties, '$.screenName') AS string
          ) = 'PURCHASES'
        )

        OR (
          event_type = 'PAYMENTS_TAB_SELECTED'
          AND UPPER(cast(
            get_json_object(event_properties, '$.screenName') AS string
          )) = 'PURCHASES'
        )
    ) AS purchases_curiosity_debug,
    
    --Settings
    COUNT(*) FILTER (
      WHERE
        event_type = 'HOME_SETTINGS_BUTTON_TAPPED'
        OR (
          event_type = 'APP_SCREEN_OPENED'
          AND cast(
            get_json_object(event_properties, '$.screenName') AS string
          ) IN ('SETTINGS', 'SUPPORT', 'SUPPORT_CONTACT_US')
        )
    ) AS settings_clicked,
    COLLECT_SET(
      named_struct(
        'event_type',
        event_type,
        'screenName',
        cast(
          get_json_object(event_properties, '$.screenName') AS string
        )
      )
    ) FILTER (
      WHERE
        event_type = 'HOME_SETTINGS_BUTTON_TAPPED'
        OR (
          event_type = 'APP_SCREEN_OPENED'
          AND cast(
            get_json_object(event_properties, '$.screenName') AS string
          ) IN ('SETTINGS', 'SUPPORT', 'SUPPORT_CONTACT_US')
        )
    ) AS settings_clicked_debug
  FROM
    {{ ref('f_amplitude_addi_funnel_project') }} a
  WHERE
    1 = 1
    AND amplitude_id IS NOT NULL
    AND _year >= 2023
    AND _month >= 1
    {% if is_incremental() -%}
    AND event_time BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "3" DAY)) AND to_date("{{ var('end_date') }}")
    {%- endif %}
    --Filtering Shop / App events
    AND (
      UPPER(platform) IN ('IOS', 'ANDROID') --Filtering Marketplace events
      OR (event_type IN (
        'VIEW_MARKETPLACE_SCREEN',
        'SELECT_MARKETPLACE_BANNER',
        'SELECT_MARKETPLACE_PRODUCT',
        'SELECT_MARKETPLACE_DEPARTMENT',
        'SELECT_MARKETPLACE_MENU',
        'SELECT_TOP_SEARCHED_KEYWORD',
        'SELECT_ADD_TO_CART',
        'SELECT_CART',
        'SELECT_MARKETPLACE_SELLER',
        'SELECT_COMPLETE_PURCHASE',
        'SELECT_CONTINUE_SHIPPING_INFORMATION',
        'SELECT_CONTINUE_MARKETPLACE_PAYMENT',
        'SELECT_QUICK_ADD_MARKETPLACE_PRODUCT',
         'SELECT_FAV_MARKETPLACE_PRODUCT'
      ))
      )
   --Excluding events from the login (this are the same we remove when counting unique users) in order to avoid having sessions with only login events
    AND event_type <> 'AUTH_REGISTRATION_SEND_CODE_TAPPED'
    AND cast(
      get_json_object(event_properties, '$.screenName') AS string
    ) NOT IN (
      'WELCOME',
      'SELECT_COUNTRY',
      'OTP',
      'LOGIN_SELECTION',
      'SINGUP',
      'SINGUP_INSTRUCTIONS',
      'PROSPECT_LOGIN',
      'NETWORK_FAILED',
      'PHONE_NUMBER_FAILED',
      'ERROR_SCREEN',
      'CODE_PUSH'
    )
  GROUP BY
    ALL
