{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

{% set dt_format = 'yyyy-MM-dd' %}
{% set dt_format_tz = 'yyyy-MM-dd 00:00:00.000Z' %}


{%- set app_addishop_channels = ('MOBILE_APP','APP','APP_BANNER','PUSH_NOTIFICATION') -%}
{%- set web_addishop_channels = ('WEB','LANDING_PAGE','WEBSITE') -%}
{%- set email_addishop_channels = ('EMAIL') -%}
{%- set facebook_addishop_channels = ('FACEBOOK') -%}
{%- set sms_addishop_channels = ('SMS') -%}
{%- set instagram_addishop_channels = ('INSTAGRAM') -%}
{%- set whatsapp_addishop_channels = ('WHATSAPP') -%}


WITH
dm_applications AS (
    SELECT *
    FROM {{ ref('dm_applications') }}
)
,
dm_originations AS (
    SELECT *
    FROM {{ ref('dm_originations') }}
)
,
d_ally_management_store_users_co AS (
    SELECT *
    FROM {{ ref('d_ally_management_store_users_co') }}
)
,
dm_originations_marketplace_suborders_co AS (
    SELECT *
    FROM {{ ref('dm_originations_marketplace_suborders_co') }}
)
,
dm_applications_marketplace_suborders_co AS (
    SELECT *
    FROM {{ ref('dm_applications_marketplace_suborders_co') }}
)
,
co_allies AS (
	SELECT
        DISTINCT
		'CO' AS country_code,
		s.ally_brand:name.value AS brand,
		s.store_name,
        s.store_slug,
		s.ally_slug,
		s.ally_channel,
		s.store_city_code,
		city_name
	FROM {{ ref('d_ally_management_stores_allies_co') }} s
	LEFT JOIN {{ ref('d_ally_management_cities_regions_co') }} c
		ON s.store_city_code = c.city_code
)
, co_addishop_originations AS (
	SELECT 
		t.application_id,
		channel AS addishop_channel
	FROM {{ source('silver_live', 'f_marketplace_transaction_attributable_co') }} t
	LEFT JOIN  {{ source('silver_live', 'f_marketplace_shopping_intents_co') }} si
		ON t.shopping_intent_id = si.shopping_intent_id
)
, co_applications_non_marketplace AS (
	SELECT
		app.country_code,
		app.ally_slug,
        app.store_slug,
		app.application_date_local,
		app.application_channel,
		CASE WHEN o.application_id is not null THEN true
		ELSE false
		END as is_origination,
		app.client_type,
		app.application_id,
		NULL AS custom_application_suborder_pairing_id,
		app.synthetic_channel AS channel,
		CASE WHEN app.synthetic_product_category='BNPN_CO' or app.synthetic_product_category is null THEN app.synthetic_product_category
		ELSE 'BNPL_CO'
		END as product_type,
		app.last_event_type_prime,
		app.ally_cluster,
		app.account_kam_name,
		sto.email AS store_user_email,
		COALESCE(o.gmv,0) AS gmv,
		0 AS marketplace_purchase_fee,
        0 AS marketplace_purchase_fee_revenue,
		COALESCE(o.ally_mdf,0) ally_mdf,
		COALESCE(o.mdf_amount,0) AS mdf_revenue,
		COALESCE(o.lead_gen_fee_rate,0) AS lead_gen_fee,
		COALESCE(o.lead_gen_fee_amount,0) AS lgf_revenue,
		o.is_addishop_referral,
		o.is_addishop_referral_paid AS is_addishop_paying_merchant
	FROM dm_applications AS app
	LEFT JOIN dm_originations AS o ON o.application_id=app.application_id
	LEFT JOIN d_ally_management_store_users_co AS sto ON sto.user_id=app.store_user_id
	WHERE 1=1
	    AND COALESCE(app.application_channel,'')  != 'ADDI_MARKETPLACE'
		AND app.country_code = 'CO' AND app.journey_name not ilike '%REFINANCE_%'
		AND app.journey_name not ilike '%PREAPPROVAL_%'
)
, co_applications_marketplace AS (
	SELECT
		app.country_code,
		COALESCE(mktplc_a.suborder_ally_slug, mktplc_o.suborder_ally_slug, app.ally_slug) AS ally_slug,
		COALESCE(mktplc_a.suborder_store_slug, mktplc_o.suborder_store_slug, app.store_slug) AS store_slug,
		app.application_date_local,
		app.application_channel,
		CASE
		    WHEN mktplc_o.custom_application_suborder_pairing_id IS NOT NULL THEN TRUE
		    ELSE FALSE
		END as is_origination,
		app.client_type,
		mktplc_a.application_id,
		mktplc_a.custom_application_suborder_pairing_id,
		app.synthetic_channel AS channel,
		CASE
		    WHEN app.synthetic_product_category='BNPN_CO' or app.synthetic_product_category IS NULL THEN app.synthetic_product_category
		    ELSE 'BNPL_CO'
		END AS product_type,
		app.last_event_type_prime,
		app.ally_cluster,
		app.account_kam_name,
		sto.email AS store_user_email,
		COALESCE(mktplc_o.synthetic_suborder_total_amount, 0) AS gmv,
		mktplc_o.suborder_marketplace_purchase_fee AS marketplace_purchase_fee,
		mktplc_o.synthetic_suborder_marketplace_purchase_fee_amount AS marketplace_purchase_fee_revenue,
		mktplc_o.suborder_origination_mdf AS ally_mdf,
		mktplc_o.synthetic_suborder_origination_mdf_amount AS mdf_revenue,
		0 AS lead_gen_fee,
		0 AS lgf_revenue,
		o.is_addishop_referral,
		o.is_addishop_referral_paid AS is_addishop_paying_merchant
    FROM      dm_applications_marketplace_suborders_co  AS mktplc_a
	LEFT JOIN dm_originations_marketplace_suborders_co  AS mktplc_o ON mktplc_a.custom_application_suborder_pairing_id = mktplc_o.custom_application_suborder_pairing_id
	LEFT JOIN dm_applications                           AS app      ON app.application_id = mktplc_a.application_id
	LEFT JOIN dm_originations                           AS o        ON o.application_id=app.application_id
	LEFT JOIN d_ally_management_store_users_co          AS sto      ON sto.user_id=app.store_user_id
	WHERE 1=1
	    AND app.application_channel  = 'ADDI_MARKETPLACE'
		AND app.country_code = 'CO' AND app.journey_name not ilike '%REFINANCE_%'
		AND app.journey_name not ilike '%PREAPPROVAL_%'
)
,
co_applications AS (
    (SELECT *
    FROM co_applications_non_marketplace)
    UNION ALL
    (SELECT *
    FROM co_applications_marketplace)
)
, co_applications_f AS (
	SELECT
		o.country_code,
		o.application_date_local,
		brand,
		o.ally_slug,
		o.ally_cluster,
		o.account_kam_name,
        o.store_slug,
		o.store_user_email,
		o.last_event_type_prime,
		o.client_type,
		o.product_type,
		o.application_channel,
		store_name,
		city_name,
		o.channel,
		SUM(o.gmv) AS gmv,
		SUM(o.mdf_revenue) AS mdf_revenue,
		SUM(o.lgf_revenue) AS lgf_revenue,
		SUM(o.marketplace_purchase_fee_revenue) AS marketplace_purchase_fee_revenue,
		SUM(o.gmv) - SUM(o.mdf_revenue) - SUM(o.lgf_revenue) - SUM(o.marketplace_purchase_fee_revenue) AS gmv_after_fee,
		COUNT(DISTINCT o.application_id) AS applications,
		COUNT(DISTINCT o.application_id) FILTER (WHERE o.is_origination is true) AS originations,
		--AddiShop Originations
		COUNT(1) FILTER (WHERE is_addishop_paying_merchant = 1) AS addishop_originations,
		COALESCE(SUM(o.gmv) FILTER (WHERE is_addishop_paying_merchant = 1),0) AS addishop_gmv,

		COUNT(1) FILTER (WHERE is_addishop_paying_merchant = 1 AND addishop_channel IN {{app_addishop_channels}} )  AS addishop_app_originations,
		COUNT(1) FILTER (WHERE is_addishop_paying_merchant = 1 AND addishop_channel IN {{web_addishop_channels}} )  AS addishop_web_originations,
		COUNT(1) FILTER (WHERE is_addishop_paying_merchant = 1 AND addishop_channel IN ("{{email_addishop_channels}}") ) AS addishop_email_originations,
		COUNT(1) FILTER (WHERE is_addishop_paying_merchant = 1 AND addishop_channel IN ("{{facebook_addishop_channels}}") ) AS addishop_facebook_originations,
		COUNT(1) FILTER (WHERE is_addishop_paying_merchant = 1 AND addishop_channel IN ("{{sms_addishop_channels}}") ) AS addishop_sms_originations,
		COUNT(1) FILTER (WHERE is_addishop_paying_merchant = 1 AND addishop_channel IN ("{{instagram_addishop_channels}}") ) AS addishop_instagram_originations,
		COUNT(1) FILTER (WHERE is_addishop_paying_merchant = 1 AND addishop_channel IN ("{{whatsapp_addishop_channels}}") ) AS addishop_whatsapp_originations,
		
		COALESCE(SUM(o.gmv) FILTER (WHERE is_addishop_paying_merchant = 1 AND addishop_channel IN {{app_addishop_channels}} ),0)  AS addishop_app_gmv,
		COALESCE(SUM(o.gmv) FILTER (WHERE is_addishop_paying_merchant = 1 AND addishop_channel IN {{web_addishop_channels}} ),0)  AS addishop_web_gmv,
		COALESCE(SUM(o.gmv) FILTER (WHERE is_addishop_paying_merchant = 1 AND addishop_channel IN ("{{email_addishop_channels}}") ),0) AS addishop_email_gmv,
		COALESCE(SUM(o.gmv) FILTER (WHERE is_addishop_paying_merchant = 1 AND addishop_channel IN ("{{facebook_addishop_channels}}") ),0) AS addishop_facebook_gmv,
		COALESCE(SUM(o.gmv) FILTER (WHERE is_addishop_paying_merchant = 1 AND addishop_channel IN ("{{sms_addishop_channels}}" )),0) AS addishop_sms_gmv,
		COALESCE(SUM(o.gmv) FILTER (WHERE is_addishop_paying_merchant = 1 AND addishop_channel IN ("{{instagram_addishop_channels}}" )),0) AS addishop_instagram_gmv,
		COALESCE(SUM(o.gmv) FILTER (WHERE is_addishop_paying_merchant = 1 AND addishop_channel IN ("{{whatsapp_addishop_channels}}" )),0) AS addishop_whatsapp_gmv
	FROM co_applications o
	LEFT JOIN co_allies a
		ON o.store_slug = a.store_slug
	LEFT JOIN co_addishop_originations 	t
		ON o.application_id = t.application_id
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)

, co_clicks AS (
	SELECT
		country_code,
		from_utc_timestamp(shopping_intent_timestamp,"America/Bogota")::date AS clicks_date,
		brand,
		s.ally_slug,
        a.store_slug,
		COUNT(1) FILTER (WHERE s.channel IN {{app_addishop_channels}} )  AS app_clicks,
		COUNT(1) FILTER (WHERE s.channel IN {{web_addishop_channels}} )  AS web_clicks,
		COUNT(1) FILTER (WHERE s.channel IN ("{{email_addishop_channels}}" )) AS email_clicks,
		COUNT(1) FILTER (WHERE s.channel IN ("{{facebook_addishop_channels}}" )) AS facebook_clicks,
		COUNT(1) FILTER (WHERE s.channel IN ("{{sms_addishop_channels}}" )) AS sms_clicks,
		COUNT(1) FILTER (WHERE s.channel IN ("{{instagram_addishop_channels}}" )) AS instagram_clicks,
		COUNT(1) FILTER (WHERE s.channel IN ("{{whatsapp_addishop_channels}}" )) AS whatsapp_clicks
	FROM {{ source('silver_live', 'f_marketplace_shopping_intents_co') }} s
	LEFT JOIN co_allies a
		ON s.ally_slug = a.ally_slug
	WHERE 1=1
	GROUP BY 1,2,3,4,5
)
, co_final AS (
SELECT
	o.*,
    COALESCE(app_clicks,0) AS app_clicks,
    COALESCE(web_clicks,0) AS web_clicks,
    COALESCE(email_clicks,0) AS email_clicks,
    COALESCE(facebook_clicks,0) AS facebook_clicks,
    COALESCE(sms_clicks,0) AS sms_clicks,
    COALESCE(instagram_clicks,0) AS instagram_clicks,
    COALESCE(whatsapp_clicks,0) AS whatsapp_clicks
FROM co_applications_f o
LEFT JOIN co_clicks f
	ON o.store_slug = f.store_slug
	AND o.application_date_local = f.clicks_date
	AND o.country_code = f.country_code
)
, br_allies AS (
	SELECT
        DISTINCT
		'BR' AS country_code,
		s.ally_brand:name.value AS brand,
		s.store_name,
        s.store_slug,
		s.ally_slug,
		s.ally_channel,
		s.store_city_code,
		city_name
	FROM {{ ref('d_ally_management_stores_allies_br') }} s
	LEFT JOIN {{ ref('d_ally_management_cities_regions_br') }} c
		ON s.store_city_code = c.city_code
)
, br_addishop_originations AS (
	SELECT 
		t.application_id,
		channel AS addishop_channel
	FROM {{ ref('f_marketplace_transaction_attributable_br') }} t
	LEFT JOIN  {{ ref('f_marketplace_shopping_intents_br') }} si
		ON t.shopping_intent_id = si.shopping_intent_id

)

, br_applications AS (
	SELECT
		app.country_code,
		app.ally_slug,
        app.store_slug,
		app.application_date_local,
		app.application_channel,
		CASE WHEN o.application_id is not null THEN true
		ELSE false
		END as is_origination,
		app.client_type,
		app.application_id,
		app.synthetic_channel AS channel,
		CASE WHEN app.synthetic_product_category='BNPN_BR' or app.synthetic_product_category is null THEN app.synthetic_product_category
		ELSE 'BNPL_BR'
		END as product_type,
		app.last_event_type_prime,
		app.ally_cluster,
		sto.email AS store_user_email,
		COALESCE(o.requested_amount,0) AS gmv,
		COALESCE(o.ally_mdf,0) ally_mdf,
		COALESCE(o.mdf_amount,0) AS mdf_revenue,
		COALESCE(o.lead_gen_fee_rate,0) AS lead_gen_fee,
		COALESCE(o.lead_gen_fee_amount,0) AS lgf_revenue,
		o.is_addishop_referral,
		o.is_addishop_referral_paid AS is_addishop_paying_merchant
	FROM {{ ref('dm_applications') }} app
	LEFT JOIN {{ ref('dm_originations') }} o ON o.application_id=app.application_id
	LEFT JOIN {{ ref('d_ally_management_store_users_br') }} sto ON sto.user_id=app.store_user_id
	WHERE 1=1
		AND app.country_code = 'BR' AND app.journey_name not ilike '%REFINANCE_%'
		AND app.journey_name not ilike '%PREAPPROVAL_%'

)
, br_applications_f AS (
	SELECT
		o.country_code,
		o.application_date_local,
		brand,
		o.ally_slug,
		o.ally_cluster,
		NULL AS account_kam_name,
        o.store_slug,
		o.store_user_email,
		o.last_event_type_prime,
		o.client_type,
		o.product_type,
		o.application_channel,
		store_name,
		city_name,
		o.channel,
		SUM(o.gmv) AS gmv,
		SUM(o.mdf_revenue) AS mdf_revenue,
		SUM(o.lgf_revenue) AS lgf_revenue,
		SUM(0) AS marketplace_purchase_fee_revenue,
		SUM(o.gmv) - SUM(o.mdf_revenue) - SUM(o.lgf_revenue) AS gmv_after_fee,
		COUNT(DISTINCT o.application_id) AS applications,
		COUNT(DISTINCT o.application_id) FILTER (WHERE o.is_origination is true) AS originations,
			--AddiShop Originations
		COUNT(1) FILTER (WHERE is_addishop_paying_merchant = 1) AS addishop_originations,
		COALESCE(SUM(o.gmv) FILTER (WHERE is_addishop_paying_merchant = 1),0) AS addishop_gmv,

		COUNT(1) FILTER (WHERE is_addishop_paying_merchant = 1 AND addishop_channel IN {{app_addishop_channels}} )  AS addishop_app_originations,
		COUNT(1) FILTER (WHERE is_addishop_paying_merchant = 1 AND addishop_channel IN {{web_addishop_channels}} )  AS addishop_web_originations,
		COUNT(1) FILTER (WHERE is_addishop_paying_merchant = 1 AND addishop_channel IN ("{{email_addishop_channels}}") ) AS addishop_email_originations,
		COUNT(1) FILTER (WHERE is_addishop_paying_merchant = 1 AND addishop_channel IN ("{{facebook_addishop_channels}}") ) AS addishop_facebook_originations,
		COUNT(1) FILTER (WHERE is_addishop_paying_merchant = 1 AND addishop_channel IN ("{{sms_addishop_channels}}") ) AS addishop_sms_originations,
		COUNT(1) FILTER (WHERE is_addishop_paying_merchant = 1 AND addishop_channel IN ("{{instagram_addishop_channels}}") ) AS addishop_instagram_originations,
		COUNT(1) FILTER (WHERE is_addishop_paying_merchant = 1 AND addishop_channel IN ("{{whatsapp_addishop_channels}}") ) AS addishop_whatsapp_originations,
		
		COALESCE(SUM(o.gmv) FILTER (WHERE is_addishop_paying_merchant = 1 AND addishop_channel IN {{app_addishop_channels}} ),0)  AS addishop_app_gmv,
		COALESCE(SUM(o.gmv) FILTER (WHERE is_addishop_paying_merchant = 1 AND addishop_channel IN {{web_addishop_channels}} ),0)  AS addishop_web_gmv,
		COALESCE(SUM(o.gmv) FILTER (WHERE is_addishop_paying_merchant = 1 AND addishop_channel IN ("{{email_addishop_channels}}") ),0) AS addishop_email_gmv,
		COALESCE(SUM(o.gmv) FILTER (WHERE is_addishop_paying_merchant = 1 AND addishop_channel IN ("{{facebook_addishop_channels}}") ),0) AS addishop_facebook_gmv,
		COALESCE(SUM(o.gmv) FILTER (WHERE is_addishop_paying_merchant = 1 AND addishop_channel IN ("{{sms_addishop_channels}}" )),0) AS addishop_sms_gmv,
		COALESCE(SUM(o.gmv) FILTER (WHERE is_addishop_paying_merchant = 1 AND addishop_channel IN ("{{instagram_addishop_channels}}" )),0) AS addishop_instagram_gmv,
		COALESCE(SUM(o.gmv) FILTER (WHERE is_addishop_paying_merchant = 1 AND addishop_channel IN ("{{whatsapp_addishop_channels}}" )),0) AS addishop_whatsapp_gmv
	FROM br_applications o
	LEFT JOIN br_allies a
		ON o.ally_slug = a.ally_slug
	LEFT JOIN br_addishop_originations 	t
		ON o.application_id = t.application_id
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)
, br_clicks AS (
	SELECT
		country_code,
		from_utc_timestamp(shopping_intent_timestamp,'America/Sao_Paulo')::date AS clicks_date,
		brand,
		s.ally_slug,
        a.store_slug,
		COUNT(1) FILTER (WHERE s.channel IN {{app_addishop_channels}} )  AS app_clicks,
		COUNT(1) FILTER (WHERE s.channel IN {{web_addishop_channels}} )  AS web_clicks,
		COUNT(1) FILTER (WHERE s.channel IN ("{{email_addishop_channels}}" )) AS email_clicks,
		COUNT(1) FILTER (WHERE s.channel IN ("{{facebook_addishop_channels}}" )) AS facebook_clicks,
		COUNT(1) FILTER (WHERE s.channel IN ("{{sms_addishop_channels}}" )) AS sms_clicks,
		COUNT(1) FILTER (WHERE s.channel IN ("{{instagram_addishop_channels}}" )) AS instagram_clicks,
		COUNT(1) FILTER (WHERE s.channel IN ("{{whatsapp_addishop_channels}}" )) AS whatsapp_clicks
	FROM {{ ref('f_marketplace_shopping_intents_br') }} s
	LEFT JOIN br_allies a
		ON s.ally_slug = a.ally_slug
	WHERE 1=1
	GROUP BY 1,2,3,4,5
)
, br_final AS (
SELECT
	o.*,
    COALESCE(app_clicks,0) AS app_clicks,
    COALESCE(web_clicks,0) AS web_clicks,
    COALESCE(email_clicks,0) AS email_clicks,
    COALESCE(facebook_clicks,0) AS facebook_clicks,
    COALESCE(sms_clicks,0) AS sms_clicks,
    COALESCE(instagram_clicks,0) AS instagram_clicks,
    COALESCE(whatsapp_clicks,0) AS whatsapp_clicks
FROM br_applications_f o
LEFT JOIN br_clicks f
	ON o.store_slug = f.store_slug
	AND o.application_date_local = f.clicks_date
	AND o.country_code = f.country_code
)
SELECT *
FROM co_final

UNION ALL

SELECT *
FROM br_final
