




WITH co_allies AS (
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
	FROM silver.d_ally_management_stores_allies_co s
	LEFT JOIN silver.d_ally_management_cities_regions_co c
		ON s.store_city_code = c.city_code
)
, co_originations AS (
	SELECT
		o.ally_slug,
        o.store_slug,
		from_utc_timestamp(o.origination_date, 'America/Bogota') AS origination_date,
		ap.client_type,
		o.application_id,
		ap.channel as application_channel,
		CASE WHEN ap.channel ILIKE '%COMM%' THEN 'E-COMMERCE' ELSE 'IN-STORE' END AS channel,
		CASE WHEN ap.requested_amount_without_discount > 0 THEN ap.requested_amount_without_discount ELSE ap.requested_amount END AS gmv,
		COALESCE(lp.ally_mdf,0) AS ally_mdf,
		CASE WHEN ap.requested_amount_without_discount > 0 THEN ap.requested_amount_without_discount * COALESCE(lp.ally_mdf,0) ELSE ap.requested_amount * COALESCE(lp.ally_mdf,0) END AS mdf_revenue,
		COALESCE(lead_gen_fee,0) AS lead_gen_fee,
		CASE WHEN ap.requested_amount_without_discount > 0 THEN ap.requested_amount_without_discount * COALESCE(lead_gen_fee,0) ELSE ap.requested_amount * COALESCE(lead_gen_fee,0) END AS lgf_revenue,
		CASE WHEN t.application_id IS NOT NULL AND la.ally_slug IS NOT NULL THEN 1 ELSE 0 END is_addishop_paying_merchant
	FROM silver.f_originations_bnpl_co o
	LEFT JOIN silver.f_applications_co ap
		ON ap.application_id = o.application_id
	LEFT JOIN silver.f_loan_proposals_co lp
		ON lp.loan_proposal_id = o.loan_id
	LEFT JOIN silver.f_marketplace_transaction_attributable_co t
		ON o.application_id = t.application_id
	LEFT JOIN gold.dm_addishop_paying_allies_co la
		ON o.ally_slug = la.ally_slug
		AND o.origination_date >= to_timestamp(la.start_date)
		AND o.origination_date <= to_timestamp(la.end_date)
	WHERE 1=1

	

    	AND to_date(from_utc_timestamp(o.origination_date, 'America/Bogota')) BETWEEN (to_date("2022-01-01"- INTERVAL "3" DAYS)) AND to_date("2022-01-30")

    

)
, co_originations_f AS (
	SELECT
		country_code,
		date_format(from_utc_timestamp(CURRENT_TIMESTAMP(), 'America/Bogota'), "yyyy-MM-dd") AS dt,
		date_format(date_trunc('day', origination_date), "yyyy-MM-dd 00:00:00.000Z") AS orig_date_tz,
		date_format(origination_date, "yyyy-MM-dd") AS orig_date,
		brand,
		a.ally_slug,
        a.store_slug,
		store_name,
		city_name,
		channel,
		SUM(gmv) AS gmv,
		SUM(mdf_revenue) AS mdf_revenue,
		SUM(lgf_revenue) AS lgf_revenue,
		SUM(gmv) - SUM(mdf_revenue) - SUM(lgf_revenue) AS gmv_after_fee,
		COALESCE(SUM(gmv) FILTER (WHERE is_addishop_paying_merchant = 1),0) AS addishop_gmv,
		COUNT(*) AS originations,
		COUNT(*) FILTER (WHERE is_addishop_paying_merchant = 1) AS addishop_originations
	FROM co_originations o
	LEFT JOIN co_allies a
		ON o.store_slug = a.store_slug
	GROUP BY 1,2,3,4,5,6,7,8,9,10
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
	FROM silver.d_ally_management_stores_allies_br s
	LEFT JOIN silver.d_ally_management_cities_regions_br c
		ON s.store_city_code = c.city_code
)
, br_originations_unified AS (
	SELECT
		application_id,
		loan_id,
		ally_slug,
        store_slug,
		origination_date
	FROM silver.f_originations_bnpl_br
	UNION ALL
	SELECT
		o.application_id,
		NULL as loan_id,
		--loan_id,
		o.ally_slug,
        a.store_slug,
		o.origination_date
	FROM silver.f_originations_bnpn_br o
    LEFT JOIN silver.f_applications_br a  ON o.application_id = a.application_id
)
, br_originations AS (
	SELECT
		o.ally_slug,
        o.store_slug,
		from_utc_timestamp(o.origination_date, 'America/Sao_Paulo') AS origination_date,
		ap.client_type,
		o.application_id,
		ap.channel as application_channel,
		CASE WHEN ap.channel ILIKE '%COMM%' THEN 'E-COMMERCE' ELSE 'IN-STORE' END AS channel,
		CASE WHEN ap.requested_amount_without_discount > 0 THEN ap.requested_amount_without_discount ELSE ap.requested_amount END AS gmv,
		COALESCE(lp.ally_mdf,0) AS ally_mdf,
		CASE WHEN ap.requested_amount_without_discount > 0 THEN ap.requested_amount_without_discount * COALESCE(lp.ally_mdf,0) ELSE ap.requested_amount * COALESCE(lp.ally_mdf,0) END AS mdf_revenue,
		0 AS lead_gen_fee,
		0 AS lgf_revenue,
		0 AS is_addishop_paying_merchant
	FROM br_originations_unified o
	LEFT JOIN silver.f_applications_br ap
		ON ap.application_id = o.application_id
	LEFT JOIN silver.f_loan_proposals_br lp
		ON lp.loan_proposal_id = o.loan_id
	WHERE 1=1

	

    	AND to_date(from_utc_timestamp(o.origination_date, 'America/Sao_Paulo')) BETWEEN (to_date("2022-01-01"- INTERVAL "3" DAYS)) AND to_date("2022-01-30")

    

)
, br_originations_f AS (
	SELECT
		country_code,
		date_format(from_utc_timestamp(CURRENT_TIMESTAMP(), 'America/Sao_Paulo'), "yyyy-MM-dd") AS dt,
		date_format(date_trunc('day', origination_date), "yyyy-MM-dd 00:00:00.000Z") AS orig_date_tz,
		date_format(origination_date, "yyyy-MM-dd") AS orig_date,
		brand,
		a.ally_slug,
        a.store_slug,
		store_name,
		city_name,
		channel,
		SUM(gmv) AS gmv,
		SUM(mdf_revenue) AS mdf_revenue,
		SUM(lgf_revenue) AS lgf_revenue,
		SUM(gmv) - SUM(mdf_revenue) - SUM(lgf_revenue) AS gmv_after_fee,
		COALESCE(SUM(gmv) FILTER (WHERE is_addishop_paying_merchant = 1),0) AS addishop_gmv,
		COUNT(*) AS originations,
		COUNT(*) FILTER (WHERE is_addishop_paying_merchant = 1) AS addishop_originations
	FROM br_originations o
	LEFT JOIN br_allies a
		ON o.ally_slug = a.ally_slug
	GROUP BY 1,2,3,4,5,6,7,8,9,10
)
, originations AS (
	SELECT * FROM co_originations_f
	UNION ALL
	SELECT * FROM br_originations_f
)
, co_clicks AS (
	SELECT
		country_code,
		date_format(from_utc_timestamp(ocurred_on_date, 'America/Bogota'), "yyyy-MM-dd") AS clicks_date,
		brand,
		s.ally_slug,
        a.store_slug,
		COUNT(*) FILTER (WHERE s.channel IN ('MOBILE_APP','APP','APP_BANNER', 'PUSH_NOTIFICATION')) AS app_clicks,
		COUNT(*) FILTER (WHERE s.channel IN ('WEB','LANDING_PAGE') ) AS web_clicks,
		COUNT(*) FILTER (WHERE s.channel IN ('EMAIL') ) AS email_clicks,
		COUNT(*) FILTER (WHERE s.channel IN ('FACEBOOK')) AS facebook_clicks,
		COUNT(*) FILTER (WHERE s.channel IN ('SMS') ) AS sms_clicks,
		COUNT(*) FILTER (WHERE s.channel IN ('INSTAGRAM') ) AS instagram_clicks,
		COUNT(*) FILTER (WHERE s.channel IN ('WHATSAPP') ) AS whatsapp_clicks
	FROM silver.f_marketplace_shopping_intents_co s
	LEFT JOIN co_allies a
		ON s.ally_slug = a.ally_slug
	WHERE 1=1
	GROUP BY 1,2,3,4,5
)
, br_clicks AS (
	SELECT
		country_code,
		date_format(from_utc_timestamp(ocurred_on_date, 'America/Sao_Paulo'), "yyyy-MM-dd") AS clicks_date,
		brand,
		s.ally_slug,
        a.store_slug,
		COUNT(*) FILTER (WHERE s.channel IN ('MOBILE_APP','APP','APP_BANNER', 'PUSH_NOTIFICATION')) AS app_clicks,
		COUNT(*) FILTER (WHERE s.channel IN ('WEB','LANDING_PAGE') ) AS web_clicks,
		COUNT(*) FILTER (WHERE s.channel IN ('EMAIL') ) AS email_clicks,
		COUNT(*) FILTER (WHERE s.channel IN ('FACEBOOK')) AS facebook_clicks,
		COUNT(*) FILTER (WHERE s.channel IN ('SMS') ) AS sms_clicks,
		COUNT(*) FILTER (WHERE s.channel IN ('INSTAGRAM') ) AS instagram_clicks,
		COUNT(*) FILTER (WHERE s.channel IN ('WHATSAPP') ) AS whatsapp_clicks
	FROM silver.f_marketplace_shopping_intents_br s
	LEFT JOIN br_allies a
		ON s.ally_slug = a.ally_slug
	WHERE 1=1
	GROUP BY 1,2,3,4,5
)
, final_clicks AS (
	SELECT * FROM co_clicks
	UNION ALL
	SELECT * FROM br_clicks
)
SELECT
	o.*,
    COALESCE(app_clicks,0) AS app_clicks,
    COALESCE(web_clicks,0) AS web_clicks,
    COALESCE(email_clicks,0) AS email_clicks,
    COALESCE(facebook_clicks,0) AS facebook_clicks,
    COALESCE(sms_clicks,0) AS sms_clicks,
    COALESCE(instagram_clicks,0) AS instagram_clicks,
    COALESCE(whatsapp_clicks,0) AS whatsapp_clicks
FROM originations o
LEFT JOIN final_clicks f
	ON o.store_slug = f.store_slug
	AND o.orig_date = f.clicks_date
	AND o.country_code = f.country_code
WHERE o.country_code = 'BR'