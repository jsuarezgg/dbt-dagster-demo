-- QUERY FOR BR

WITH br_applications AS (
    SELECT
        ocurred_on_date,
        client_id,
        journey_name,
        ally_slug,
        MAX(down_payment_br_out) as down_payment_br_out
    FROM gold.dm_application_process_funnel_br
    WHERE 1=1
        AND client_type IN ('CLIENT')
        AND ocurred_on_date > date_add(current_date(),-30)
    GROUP BY 1,2,3,4
)
, br_loans AS (
SELECT
	ally_slug,
	COUNT(loan_id) FILTER (WHERE o.origination_date > date_add(current_date(),-30)) AS originations_orig,
	SUM(approved_amount) FILTER (WHERE o.origination_date > date_add(current_date(),-30)) AS gmv,
	SUM(approved_amount) FILTER (WHERE o.origination_date > date_add(current_date(),-30))/ COUNT(loan_id) FILTER (WHERE o.origination_date > date_add(current_date(),-30)) AS aov,
	SUM(CASE WHEN t.application_id IS NOT NULL THEN 1 ELSE 0 END) FILTER (WHERE o.origination_date > date_add(current_date(),-30)) as app_originations,
	SUM(CASE WHEN t.application_id IS NOT NULL THEN approved_amount ELSE 0 END) FILTER (WHERE o.origination_date > date_add(current_date(),-30)) as app_gmv,
	SUM(CASE WHEN t.application_id IS NOT NULL THEN approved_amount ELSE 0 END) FILTER (WHERE o.origination_date > date_add(current_date(),-30)) 
	/ SUM(CASE WHEN t.application_id IS NOT NULL THEN 1 ELSE 0 END) FILTER (WHERE o.origination_date > date_add(current_date(),-30)) AS app_aov,
	date_diff(current_date(),MIN(o.origination_date)) AS daysSinceFirstOrigination
FROM (
			SELECT	
					loan_id,
					origination_date,
					ally_slug, 
					approved_amount, 
					application_id
			FROM  silver.f_originations_bnpl_br
			UNION ALL
			SELECT	
					application_id AS loan_id,
					origination_date,
					ally_slug, 
					requested_amount AS approved_amount, 
					application_id
				FROM  silver.f_originations_bnpn_br
			) o
LEFT JOIN silver.f_marketplace_transaction_attributable_br t
	ON t.application_id = o.application_id
WHERE 1=1
GROUP BY 1
)
,
app_clicks AS (
    SELECT
        COALESCE(ally_name, ally_slug) AS ally_slug,
        COUNT(*) as app_clicks
    FROM gold.dm_amplitude
    WHERE 1=1
        AND upper(event_type) IN (  'HOME_STORE_TAPPED',
                                    'SHOP_STORE_TAPPED',
                                    'AMPLITUDE_ADDI_FUNNEL_HOME_STORE_TAPPED',
                                    'STORE_TAPPED',
                                    'STORE_TAPPED_FROM_SHOP')
        AND source = 'MOBILE_APP'
        AND event_date > date_add(current_date(),-30)
    GROUP BY 1
)
, component_app_originations AS (
SELECT
	ally_slug,
	loan_id,
	origination_datetime,
	explode(a.sources_2days) AS events
FROM gold.dm_addi_okr_metrics_transactions_starting_addi_app a
WHERE 1=1
	AND app_purchase_criteria_2days = 1
	AND origination_datetime > date_add(current_date(),-7)
	AND country_code = 'BR'
)
, main_store_originations AS (
SELECT 
	ally_slug, 
	COUNT(*) AS MainStoreListOriginationsLast7Days 
FROM component_app_originations
WHERE 1=1
	AND lower(events['screen']) = 'home'
	AND	lower(events['component']) = 'main-store-list'
GROUP BY 1
)

, metrics AS (
SELECT
	a.ally_slug,
	0 as paying_merchant,
	COUNT(*) as applications,
	SUM(down_payment_br_out) as originations,
	originations_orig,
	SUM(down_payment_br_out) / COUNT(*) as rc_e2e,
	gmv,
	aov,
	app_originations,
	app_clicks,
	app_gmv,
	COALESCE(app_aov,0) AS app_aov,
	daysSinceFirstOrigination,
	MainStoreListOriginationsLast7Days
FROM br_applications a
LEFT JOIN br_loans l
	ON a.ally_slug = l.ally_slug
LEFT JOIN main_store_originations mso
	ON a.ally_slug = mso.ally_slug
LEFT JOIN app_clicks c
	ON a.ally_slug = c.ally_slug
GROUP BY 1,2,5,7,8,9,10,11,12,13,14
)
, final_metrics AS (
SELECT 
	*,
	ROW_NUMBER() OVER (ORDER BY paying_merchant DESC, originations DESC) AS OrigLast30_AddiShopPaying_Score,
	ROW_NUMBER() OVER (ORDER BY paying_merchant DESC, MainStoreListOriginationsLast7Days DESC, originations DESC) AS MainStoreOrigLast7_AddiShopPaying_Score
FROM metrics m
) 

 
	SELECT
		ally_slug,
		'applications' AS metric,
		applications AS value
	FROM final_metrics

	UNION ALL 
	 
	SELECT
		ally_slug,
		'originations' AS metric,
		originations AS value
	FROM final_metrics

	UNION ALL 
	 
	SELECT
		ally_slug,
		'rc_e2e' AS metric,
		rc_e2e AS value
	FROM final_metrics

	UNION ALL 
	 
	SELECT
		ally_slug,
		'gmv' AS metric,
		gmv AS value
	FROM final_metrics

	UNION ALL 
	 
	SELECT
		ally_slug,
		'aov' AS metric,
		aov AS value
	FROM final_metrics

	UNION ALL 
	 
	SELECT
		ally_slug,
		'app_originations' AS metric,
		app_originations AS value
	FROM final_metrics

	UNION ALL 
	 
	SELECT
		ally_slug,
		'app_clicks' AS metric,
		app_clicks AS value
	FROM final_metrics

	UNION ALL 
	 
	SELECT
		ally_slug,
		'app_gmv' AS metric,
		app_gmv AS value
	FROM final_metrics

	UNION ALL 
	 
	SELECT
		ally_slug,
		'app_aov' AS metric,
		app_aov AS value
	FROM final_metrics

	UNION ALL 
	 
	SELECT
		ally_slug,
		'paying_merchant' AS metric,
		paying_merchant AS value
	FROM final_metrics

	UNION ALL 
	 
	SELECT
		ally_slug,
		'daysSinceFirstOrigination' AS metric,
		daysSinceFirstOrigination AS value
	FROM final_metrics

	UNION ALL 
	 
	SELECT
		ally_slug,
		'OrigLast30_AddiShopPaying_Score' AS metric,
		OrigLast30_AddiShopPaying_Score AS value
	FROM final_metrics

	UNION ALL 
	 
	SELECT
		ally_slug,
		'MainStoreOrigLast7_AddiShopPaying_Score' AS metric,
		MainStoreOrigLast7_AddiShopPaying_Score AS value
	FROM final_metrics

	/*SELECT
	ally_slug,
	'ApplicationsLast30Days' AS metric,
	applications AS value
FROM final_metrics

UNION ALL

SELECT
	ally_slug,
	'OriginationsLast30Days' AS metric,
	originations AS value
FROM final_metrics

UNION ALL

SELECT
	ally_slug,
	'RcE2eLast30Days' AS metric,
	rc_e2e AS value
FROM final_metrics

UNION ALL

SELECT
	ally_slug,
	'GmvLast30Days' AS metric,
	gmv AS value
FROM final_metrics

UNION ALL

SELECT
	ally_slug,
	'AovLast30Days' AS metric,
	aov AS value
FROM final_metrics

UNION ALL

SELECT
	ally_slug,
	'AppOriginationsLast30Days' AS metric,
	app_originations AS value
FROM final_metrics

UNION ALL

SELECT
	ally_slug,
	'AppClicksLast30Days' AS metric,
	app_clicks AS value
FROM final_metrics

UNION ALL

SELECT
	ally_slug,
	'AppGmvLast30Days' AS metric,
	app_gmv AS value
FROM final_metrics

UNION ALL

SELECT
	ally_slug,
	'AppAovLast30Days' AS metric,
	app_aov AS value
FROM final_metrics

UNION ALL

SELECT
	ally_slug,
	'PayingMerchant' AS metric,
	paying_merchant AS value
FROM final_metrics

UNION ALL

SELECT
	ally_slug,
	'daysSinceFirstOrigination' AS metric,
	daysSinceFirstOrigination AS value
FROM final_metrics

UNION ALL

SELECT
	ally_slug,
	'ScoreA (Orig. Last 30 days)' AS metric,
	OrigLast30_AddiShopPaying_Score AS value
FROM final_metrics

UNION ALL

SELECT
	ally_slug,
	'ScoreB (Main Store List Orig. Last 7 days' AS metric,
	MainStoreOrigLast7_AddiShopPaying_Score AS value
FROM final_metrics
*/