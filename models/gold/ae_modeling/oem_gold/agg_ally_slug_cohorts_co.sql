{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

-- Dec / 2023 - Further context on Asana Task: [Training+Adhoc] Ally Datamart Creation
-- Link: https://app.asana.com/0/1201097266830049/1205531586806110/f

WITH
bl_ally_slug AS (
	-- CO Ally variables
    SELECT
    	country_code,
        ally_slug,
        ally_brand,
        ally_cluster,
        ally_vertical
    FROM {{ ref('bl_ally_brand_ally_slug_status') }}
  WHERE country_code = 'CO'
)
,
dm_originations AS (
	-- CO Ally GMV results
	SELECT
		ally_slug,
		(origination_date_local::DATE) AS origination_date_local,
		SUM(gmv) AS gmv

	FROM {{ ref('dm_originations') }}
	WHERE country_code ='CO'
	-- WHERE (origination_date_local::DATE) < (FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Bogota'))::DATE
	GROUP BY 1,2
)
,
dm_ally_slug_co AS (
	-- CO Ally Slug key timestamps and custom variable: `custom_ready_to_transact_date_local`
    SELECT
        ally_slug,
        min_application_date_local,
        max_application_date_local,
        min_origination_date_local,
        max_origination_date_local,
        min_terms_and_conditions_acceptance_date_local::date AS min_terms_and_conditions_acceptance_date_local,
        LEAST(min_terms_and_conditions_acceptance_date_local,min_application_date_local) AS custom_ready_to_transact_date_local,
        CASE
            WHEN LEAST(min_terms_and_conditions_acceptance_date_local,min_application_date_local) = min_terms_and_conditions_acceptance_date_local THEN 'T&C_START'
            WHEN LEAST(min_terms_and_conditions_acceptance_date_local,min_application_date_local) = min_application_date_local THEN 'APP_DATE_START'
            ELSE '_ERROR_'
        END AS custom_ready_to_transact_type
    FROM {{ ref('dm_ally_slug_co') }}
    --WHERE LEAST(min_terms_and_conditions_acceptance_date_local,min_application_date_local)::date > ('2022-01-01')::date
)
,
fx_rates(
	-- CO Official FX Rates
	SELECT
		SUM(price) FILTER(WHERE country_code='CO') AS fx_rate_cop,
		SUM(price) FILTER(WHERE country_code='BR') AS fx_rate_brl
	FROM {{ source('silver', 'd_fx_rate') }}
	WHERE is_active
)
,
daily_calendar_series AS (
	-- Generic daily time series from 2019 up to yesterday (COT)
    SELECT
        day_::date AS calendar_day
    FROM {{ source('gold', 'dm_support_daily_calendar_series') }}
    WHERE (day_::DATE)>('2019-01-01')::DATE AND  (day_::DATE) < (FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Bogota'))::DATE
)
,
ally_slug_co_daily_series AS (
	-- CO Ally slug time series using `custom_ready_to_transact_date_local` and the daily_calendar_series
    SELECT
        dcs.calendar_day,
        als.ally_slug,
        als.custom_ready_to_transact_date_local,
        als.custom_ready_to_transact_type,
        (dcs.calendar_day-als.custom_ready_to_transact_date_local)::int AS day_since_ready_to_transact,
        COALESCE(o.gmv,0) AS gmv
    FROM      dm_ally_slug_co        AS als
    LEFT JOIN daily_calendar_series  AS dcs ON als.custom_ready_to_transact_date_local <= dcs.calendar_day
    LEFT JOIN dm_originations        AS o   ON als.ally_slug = o.ally_slug AND dcs.calendar_day = o.origination_date_local
	--ORDER BY 2,1 asc
)
,
ally_slug_co_daily_series_with_periods_data AS (
	-- CO Ally slug time series relative calculations in reference to `custom_ready_to_transact_date_local`, preparing for weekly and monthly time series
    SELECT
        calendar_day,
		date_format(calendar_day, 'EEEE') AS calendar_day_day_of_week,
        ally_slug,
        custom_ready_to_transact_date_local,
        custom_ready_to_transact_type,
		day_since_ready_to_transact,
		CEIL((day_since_ready_to_transact+0.01)/7) AS week_since_ready_to_transact, -- 0.01 to correctly assign day 0 and starts of week
		COUNT(calendar_day) OVER (PARTITION BY ally_slug, CEIL((day_since_ready_to_transact+0.01)/7))=7 AS week_is_complete,
		CEIL((day_since_ready_to_transact+0.01)/30) AS month_since_ready_to_transact, -- 0.01 to correctly assign day 0 and starts of week
		COUNT(calendar_day) OVER (PARTITION BY ally_slug, CEIL((day_since_ready_to_transact+0.01)/30))=30 AS month_is_complete,
        gmv
    FROM ally_slug_co_daily_series
	--ORDER BY 3,1 asc
)
,
ally_slug_co_weely_series AS (
	-- CO Ally slug WEEKLY time series
	SELECT
		ally_slug,
		custom_ready_to_transact_date_local,
        custom_ready_to_transact_type,
		'WEEK' AS period_type,
		week_since_ready_to_transact AS period_since_ready_to_transact,
		MIN(calendar_day) AS relative_period_start_date_local,
		MAX(calendar_day) AS relative_period_end_date_local,
		SUM(gmv) AS gmv
	FROM ally_slug_co_daily_series_with_periods_data
	WHERE week_is_complete
	GROUP BY 1,2,3,4,5
	--ORDER BY 1,5 asc
)
,
ally_slug_co_monthly_series AS (
	-- CO Ally slug MONTHLY time series
	SELECT
		ally_slug,
		custom_ready_to_transact_date_local,
        custom_ready_to_transact_type,
		'MONTH' AS period_type,
		month_since_ready_to_transact AS period_since_ready_to_transact,
		MIN(calendar_day) AS relative_period_start_date_local,
		MAX(calendar_day) AS relative_period_end_date_local,
		SUM(gmv) AS gmv
	FROM ally_slug_co_daily_series_with_periods_data
	WHERE month_is_complete
	GROUP BY 1,2,3,4,5
	--ORDER BY 1,5 asc
)
,
ally_slug_co_both_series AS (
	-- CO Ally slug BOTH WEEKLY and MONTHLY time series
	SELECT * FROM ally_slug_co_weely_series
	UNION ALL
	SELECT * FROM ally_slug_co_monthly_series
)

-- FINAL query, bring additional variables relative
SELECT
	MD5(CONCAT(asbs.ally_slug, asbs.period_type, STRING(asbs.period_since_ready_to_transact))) AS dataset_surrogate_key,
	'CO' AS country_code,
	-- RELEVANT VARIABLES FOR ANALYSIS
	asbs.ally_slug,
	asbs.custom_ready_to_transact_date_local,
	asbs.custom_ready_to_transact_type,
	asbs.period_type,
	asbs.period_since_ready_to_transact,
	asbs.relative_period_start_date_local,
	asbs.relative_period_end_date_local,
	-- KEY COHORT VARIABLE
	asbs.gmv,
	-- VARIABLES FOR FILTERING
    bl_als.ally_brand,
    bl_als.ally_cluster,
    LOWER(bl_als.ally_vertical) AS ally_vertical,
	-- VARIABLES FOR RATE CONVERSION
    fx.fx_rate_cop,
    fx.fx_rate_brl,
	-- VARIABLES FOR DEBUGGING
    dm_als.min_application_date_local,
    dm_als.max_application_date_local,
    dm_als.min_origination_date_local,
    dm_als.max_origination_date_local,
    dm_als.min_terms_and_conditions_acceptance_date_local
FROM      ally_slug_co_both_series AS asbs
LEFT JOIN dm_ally_slug_co          AS dm_als ON dm_als.ally_slug = asbs.ally_slug
LEFT JOIN bl_ally_slug             AS bl_als ON asbs.ally_slug   = bl_als.ally_slug
LEFT JOIN fx_rates                 AS fx     ON TRUE
ORDER BY 3,8 asc