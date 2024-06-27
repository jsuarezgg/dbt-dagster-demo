-- QUERY FOR CO

WITH co_applications AS (
    SELECT
        ocurred_on_date,
        client_id,
        journey_name,
        ally_slug,
        GREATEST(MAX(loan_acceptance_co_out),MAX(loan_acceptance_v2_co_out)) as loan_acceptance_co_out
    FROM gold.dm_application_process_funnel_co
    WHERE 1=1
        AND journey_name IN ('CLIENT_PAGO_CO', 'CLIENT_FINANCIA_CO','CLIENT_PAGO_V2_CO')
        AND ocurred_on_date > date_add(current_date(),-30)
    GROUP BY 1,2,3,4
)
,
co_loans AS (
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
    FROM silver.f_originations_bnpl_co AS o
    LEFT JOIN silver.f_marketplace_transaction_attributable_co AS t
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
,
allies_marketplace AS (
    SELECT
        distinct ally_slug
    FROM gold.dm_addishop_paying_allies_co
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
    AND country_code = 'CO'
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
	CASE WHEN p.ally_slug IS NOT NULL THEN 1 ELSE 0 END AS paying_merchant,
	COUNT(*) as applications,
	SUM(loan_acceptance_co_out) as originations,
	originations_orig,
	SUM(loan_acceptance_co_out) / COUNT(*) as rc_e2e,
	gmv,
	aov,
	app_originations,
	app_clicks,
	app_gmv,
	COALESCE(app_aov,0) AS app_aov,
	daysSinceFirstOrigination,
	MainStoreListOriginationsLast7Days
	
FROM co_applications a
LEFT JOIN co_loans l
	ON a.ally_slug = l.ally_slug
LEFT JOIN app_clicks c
	ON a.ally_slug = c.ally_slug
LEFT JOIN main_store_originations mso
	ON a.ally_slug = mso.ally_slug
LEFT JOIN (SELECT distinct ally_slug FROM gold.dm_addishop_paying_allies_co  WHERE current_status = 'Active') p
	ON a.ally_slug = p.ally_slug
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

	