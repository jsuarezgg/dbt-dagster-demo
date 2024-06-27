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


{%- set click_events = ( "HOME_STORE_TAPPED",
                    "SHOP_STORE_TAPPED",
                    "HOME_PRODUCT_TAPPED",
                    "HOME_PROMOTED_BANNER_TAPPED",
                    'SELECT_STORE',
                    'SELECT_DEAL'
                    ) -%}

{%- set opened_screens = ( "HOME",
                    "CATEGORY",
                    "SHOP_NOW",
                    "STORES",
                    "SUBCATEGORY",
                    "SUB_CATEGORY",
                    "DEALS",
                    "PREFERRED",
                    "SUGGESTED_ALLIES_SCREEN",
                    "HOW_TO_BUY_MODAL"
                    ) -%}

WITH app_events AS (
  SELECT 
      event_type,
      session_id,
      event_id,
      unique_id,
      user_id AS client_id,
      amplitude_id,
      device_id,
      COALESCE(a.ally_name, a.ally_slug, a.event_properties:['allySlug']) AS ally_slug,
      b.ally_brand,
      b.ally_cluster,
      b.ally_vertical,
      to_date(event_time) AS client_event_date,
      to_timestamp(event_time) AS client_event_time,
      from_utc_timestamp( to_timestamp(event_time),'America/Bogota') AS client_event_time_local,
      CASE
        WHEN (cast(a.event_properties:['screenName'] as string) ILIKE '%regalo%' OR
              cast(a.event_properties:['screenName'] as string) ILIKE '%amistad%' OR
              cast(a.event_properties:['screenName'] as string) ILIKE '%amigo%')
            THEN 'DIA_AMISTAD'
        WHEN  cast(a.event_properties:['screenName'] as string) = 'SUB_CATEGORY'
            THEN 'SUBCATEGORY'
        ELSE cast(a.event_properties:['screenName'] as string) END AS screen_name,
      COALESCE(cast(a.event_properties:['componentName'] as string), cast(a.event_properties:['origin'] as string)) AS component_name,
      a.event_properties:['categoryName'] AS category_name,
      a.event_properties:['subcategoryName'] AS sub_category_name,
      a.event_properties:['position'] AS  click_position
  FROM {{ ref('f_amplitude_addi_funnel_project') }} a
  LEFT JOIN {{ ref('bl_ally_brand_ally_slug_status') }} b
        ON  COALESCE(a.ally_name, a.ally_slug, a.event_properties:['allySlug']) = b.ally_slug
        AND b.country_code = 'CO'
  WHERE 1=1
    AND upper(event_type) IN {{click_events}}
    --AND user_id IS NOT NULL 
    --AND COALESCE(ally_name, cast(get_json_object(event_properties,'$.allySlug') as string)) IS NOT NULL 
    AND to_date(event_date) >= '2022-06-03'
    --AND lower(country) = 'colombia'
    AND COALESCE(upper( event_properties:['source']),upper(source)) = 'MOBILE_APP'
) 

, addishop_applications AS (
  SELECT
    ap.client_id,
    ap.device_id,
    ap.application_id,
    ap.original_product,
    CASE WHEN ap.ally_slug = 'decathlon-online' THEN 'decathlon-ecommerce' ELSE ap.ally_slug END AS ally_slug, --handle decathlon attribution
    ap.application_datetime,
    ap.application_datetime_local,
    ap.is_addishop_referral,
    ap.is_addishop_referral_paid
  FROM  {{ ref('dm_applications') }} ap
  WHERE 1=1
        AND ap.is_addishop_referral IS TRUE
        AND ap.addishop_channel IN ('APP', 'MOBILE_APP') --we´re not storing this data in the applicationCreated event
        AND to_date(application_datetime) >= '2022-06-03'
        AND ap.country_code = 'CO'
)
, addishop_originations AS (
    SELECT 
        client_id,
        application_id,
        loan_id,
        CASE WHEN ally_slug = 'decathlon-online' THEN 'decathlon-ecommerce' ELSE ally_slug END AS ally_slug, --handle decathlon attribution
        is_addishop_referral,
        is_addishop_referral_paid,
        lead_gen_fee_rate,
        gmv/fx_rate AS gmv_usd,
        lead_gen_fee_amount/fx_rate AS lead_gen_fee_amount_usd,
        origination_date,
        origination_date_local
    FROM  {{ ref('dm_originations') }}
    WHERE 1=1
      AND country_code = 'CO'
      AND is_addishop_referral IS TRUE
      AND addishop_channel IN ('APP', 'MOBILE_APP')
      AND to_date(origination_date_local) >= '2022-06-03'
)
, addishop_app_orig AS (
SELECT
    a.*,
    (application_datetime_local - interval 2 day) AS application_datetime_local_interval_2_days,
    loan_id,
    CASE WHEN o.application_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_origination,
    o.is_addishop_referral AS origination_is_addishop_referral,
    o.is_addishop_referral_paid AS origination_is_addishop_referral_paid,
    lead_gen_fee_rate,
    gmv_usd,
    lead_gen_fee_amount_usd,
    origination_date,
    origination_date_local
FROM addishop_applications a
LEFT JOIN addishop_originations o
  ON a.application_id = o.application_id
)
, final_table AS (
SELECT
  ae.client_id,
  ae.amplitude_id,
  client_event_time_local AS client_event_time,
  session_id,
  unique_id,
  ae.ally_slug,
  ae.ally_brand,
  ae.ally_cluster,
  ae.ally_vertical,
  event_type,
  CASE WHEN ae.screen_name IN ("Black Friday",
                                 "Más descuentos",
                                  "Cyber Monday",
                                  "Envío gratis en Black Friday",
                                  "Addi Ofertas",
                                  "Envío gratis",
                                  "SMB Test",
                                  "Pequeños comercios, grandes descuentos",
                                  "Pequeños comercios",
                                  "10% OFF using ADDI10 in checkout")  
       THEN 'MARKETING_CAMPAIGN'        
       ELSE ae.screen_name 
  END AS screen_name,      
  component_name,
  ae.category_name,
  ae.sub_category_name,
  click_position,
  CASE WHEN application_id IS NOT NULL THEN (CASE WHEN row_number() OVER (PARTITION BY application_id ORDER BY client_event_time DESC) = 1 THEN 1 ELSE 0 END)
  END AS is_last_click_application,
  ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY client_event_time DESC) AS app_event_order,
  application_id,
  original_product,
  application_datetime,
  application_datetime_local,
  is_addishop_referral AS application_is_addishop_referral,
  is_addishop_referral_paid AS application_is_addishop_referral_paid,
  is_origination,
  loan_id,
  origination_is_addishop_referral,
  origination_is_addishop_referral_paid,
  lead_gen_fee_rate,
  gmv_usd,
  lead_gen_fee_amount_usd,
  origination_date,
  origination_date_local
FROM app_events ae
LEFT JOIN addishop_app_orig aao
  ON ae.client_id = aao.client_id
  AND ae.ally_slug = aao.ally_slug
  AND ae.client_event_time_local  < aao.application_datetime_local -- APP event timestamp BEFORE origination timestamp
  AND ae.client_event_time_local  >= aao.application_datetime_local_interval_2_days -- APP event IN INTERVAL 


UNION

SELECT
  ae.client_id,
  ae.amplitude_id,
  client_event_time_local AS client_event_time,
  session_id,
  unique_id,
  ae.ally_slug,
  ae.ally_brand,
  ae.ally_cluster,
  ae.ally_vertical,
  event_type,
  CASE WHEN ae.screen_name IN ("Black Friday",
                                 "Más descuentos",
                                  "Cyber Monday",
                                  "Envío gratis en Black Friday",
                                  "Addi Ofertas",
                                  "Envío gratis",
                                  "SMB Test",
                                  "Pequeños comercios, grandes descuentos",
                                  "Pequeños comercios",
                                  "10% OFF using ADDI10 in checkout")  
       THEN 'MARKETING_CAMPAIGN'        
       ELSE ae.screen_name 
  END AS screen_name,       
  component_name,
  ae.category_name,
  ae.sub_category_name,
  click_position,
  CASE WHEN application_id IS NOT NULL THEN (CASE WHEN row_number() OVER (PARTITION BY application_id ORDER BY client_event_time DESC) = 1 THEN 1 ELSE 0 END)
  END AS is_last_click_application,
  ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY client_event_time DESC) AS app_event_order,
  application_id,
  original_product,
  application_datetime,
  application_datetime_local,
  is_addishop_referral AS application_is_addishop_referral,
  is_addishop_referral_paid AS application_is_addishop_referral_paid,
  is_origination,
  loan_id,
  origination_is_addishop_referral,
  origination_is_addishop_referral_paid,
  lead_gen_fee_rate,
  gmv_usd,
  lead_gen_fee_amount_usd,
  origination_date,
  origination_date_local
FROM app_events ae
LEFT JOIN addishop_app_orig aao
  ON ae.device_id = aao.device_id
  AND ae.ally_slug = aao.ally_slug
  AND ae.client_event_time_local  < aao.application_datetime_local -- APP event timestamp BEFORE origination timestamp
  AND ae.client_event_time_local  >= aao.application_datetime_local_interval_2_days -- APP event IN INTERVAL 
)
SELECT 
  surrogate_key,
  client_id,
  amplitude_id,
  client_event_time,
  session_id,
  unique_id,
  ally_slug,
  ally_brand,
  ally_cluster,
  ally_vertical,
  event_type,
  screen_name,       
  component_name,
  category_name,
  sub_category_name,
  click_position,
  is_last_click_application,
  app_event_order,
  application_id,
  original_product,
  application_datetime,
  application_datetime_local,
  application_is_addishop_referral,
  application_is_addishop_referral_paid,
  is_origination,
  loan_id,
  origination_is_addishop_referral,
  origination_is_addishop_referral_paid,
  lead_gen_fee_rate,
  gmv_usd,
  lead_gen_fee_amount_usd,
  origination_date,
  origination_date_local
FROM (
        SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY cast(concat(coalesce(cast(unique_id as string), ''), '-', coalesce(cast(app_event_order as string), '')) as string) ORDER BY application_id DESC) AS row_num,
        md5(cast(concat(coalesce(cast(unique_id as 
            string
        ), ''), '-', coalesce(cast(app_event_order as 
            string
        ), '')) as 
            string
        )) AS surrogate_key
        FROM final_table
        WHERE 1=1
{% if is_incremental() %}
    AND to_date(client_event_time) BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(client_event_time AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
)
WHERE row_num = 1
