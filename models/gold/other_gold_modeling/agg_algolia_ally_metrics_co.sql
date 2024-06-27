{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

{%- set days_before = 30 -%}

{%- set metrics = [ "applications",
                    "originations",
                    "originations_orig",
                    "rc_e2e",
                    "gmv",
                    "aov",
                    "app_originations",
                    "app_clicks",
                    "app_gmv",
                    "app_aov",
                    "paying_merchant",
                    "daysSinceFirstOrigination",
                    "OrigLast30_AddiShopPaying_Score",
                    "MainStoreOrigLast7_AddiShopPaying_Score"
                    ] -%}

-- QUERY FOR CO

WITH co_applications AS (
  SELECT
    CASE WHEN ally_slug = 'decathlon-online' THEN 'decathlon-ecommerce' ELSE ally_slug END AS ally_slug,
    SUM(sum_stage_in) AS applications,
    SUM(sum_stage_out) / SUM(sum_stage_in) AS rc_e2e
  FROM {{ ref('agg_application_process_funnel_co') }}
  WHERE 1=1 
    AND stage = 'E2E'
    AND client_type = 'CLIENT'
    AND ocurred_on_date > date_add(current_date(),-30)
  GROUP BY 1
)
,
co_loans AS (
    SELECT
        CASE WHEN ally_slug = 'decathlon-online' THEN 'decathlon-ecommerce' ELSE ally_slug END AS ally_slug,
        COUNT(loan_id) FILTER (WHERE o.origination_date > date_add(current_date(),-30)) AS originations_orig,
        SUM(gmv) FILTER (WHERE o.origination_date > date_add(current_date(),-30)) AS gmv,
        SUM(gmv) FILTER (WHERE o.origination_date > date_add(current_date(),-30))/ COUNT(loan_id) FILTER (WHERE o.origination_date > date_add(current_date(),-30)) AS aov,
        COUNT(distinct loan_id) FILTER (WHERE o.is_addishop_referral IS TRUE AND o.origination_date_local > date_add(current_date(),-30)) as app_originations,
        SUM(gmv) FILTER (WHERE o.is_addishop_referral IS TRUE AND o.origination_date_local > date_add(current_date(),-30)) as app_gmv,
        SUM(gmv) FILTER (WHERE o.is_addishop_referral IS TRUE AND o.origination_date_local > date_add(current_date(),-30))
        / COUNT(distinct loan_id) FILTER (WHERE o.is_addishop_referral IS TRUE AND o.origination_date_local > date_add(current_date(),-30)) AS app_aov,
        date_diff(current_date(),MIN(o.origination_date)) AS daysSinceFirstOrigination
    FROM {{ ref('dm_originations') }} AS o
    WHERE country_code = 'CO'
    GROUP BY 1
)
,
app_clicks AS (
    SELECT
        COALESCE(ally_name, ally_slug) AS ally_slug,
        COUNT(*) as app_clicks
    FROM {{ ref('f_amplitude_addi_funnel_project') }} a
    WHERE 1=1
        AND upper(event_type) IN (  'HOME_STORE_TAPPED',
                                    'SHOP_STORE_TAPPED',
                                    'AMPLITUDE_ADDI_FUNNEL_HOME_STORE_TAPPED',
                                    'STORE_TAPPED',
                                    'STORE_TAPPED_FROM_SHOP',
                                    'SELECT_STORE',
                                    'SELECT_DEAL')
        AND COALESCE(upper( event_properties:['source']),upper(source)) = 'MOBILE_APP'
        AND event_date > date_add(current_date(),-30)
    GROUP BY 1
)

, main_store_originations AS (
    SELECT
        CASE WHEN ally_slug = 'decathlon-online' THEN 'decathlon-ecommerce' ELSE ally_slug END AS ally_slug,
        COUNT(*) AS MainStoreListOriginationsLast7Days
    FROM {{ ref('dm_shopping_intent_application_origination_co') }} a
    WHERE 1=1
        AND is_origination_flag IS TRUE
        AND shopping_intent_timestamp_local > date_add(current_date(),-7)
        AND lower(screen) = 'home'
        AND	lower(component) = 'main-store-list'
    GROUP BY 1
)
, metrics AS (
SELECT
	a.ally_slug,
    --CASE WHEN a.ally_slug = 'decathlon-online' THEN 'decathlon-ecommerce' ELSE a.ally_slug END AS ally_slug,
	CASE WHEN p.ally_slug IS NOT NULL THEN 1 ELSE 0 END AS paying_merchant,
	applications,
	originations_orig as originations,
    originations_orig,
	rc_e2e,
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
LEFT JOIN (SELECT distinct ally_slug FROM  {{ ref('dm_addishop_paying_allies_co') }}  WHERE current_status = 'Active') p
	ON a.ally_slug = p.ally_slug
)
, final_metrics AS (
SELECT 
	*,
	ROW_NUMBER() OVER (ORDER BY paying_merchant DESC, originations_orig DESC) AS OrigLast30_AddiShopPaying_Score,
	ROW_NUMBER() OVER (ORDER BY paying_merchant DESC, MainStoreListOriginationsLast7Days DESC, originations DESC) AS MainStoreOrigLast7_AddiShopPaying_Score
FROM metrics m
) 

{% for metric in metrics %} 
	SELECT
		ally_slug,        
        '{{metric}}' AS metric,
        {{metric}} AS value
	FROM final_metrics

	{% if not loop.last -%}
	UNION ALL 
	{% endif -%}

{% endfor -%}