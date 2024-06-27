{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH co_addishop_paying_slugs AS (
  SELECT distinct ally_slug 
  FROM {{ ref('dm_addishop_paying_allies_co') }}
)
, state_ally_management AS (
    SELECT 
        ally_slug,
        ally_state,
        ocurred_on AS start_date,
        COALESCE(LAG(ocurred_on) OVER (PARTITION BY ally_slug ORDER BY ocurred_on DESC),to_timestamp('2050-01-01 00:00:00')) AS end_date
    FROM {{ ref('d_ally_slugs_co_logs') }}
    WHERE 1=1
        AND event_name IN ('AllyUpdated', 'AllyCreated')
        AND ally_slug IN (SELECT distinct ally_slug FROM co_addishop_paying_slugs)
) 

, state_status_calendar AS (
    SELECT distinct
        cs.day_ AS period,
        sm.ally_slug,
        sm.ally_state
    FROM {{ source('gold', 'dm_support_daily_calendar_series') }} cs
    LEFT JOIN state_ally_management sm
        ON cs.day_ >= sm.start_date
        AND cs.day_ <= sm.end_date 
    WHERE 1=1
        AND sm.ally_slug IS NOT NULL
        AND cs.day_ >= '2022-09-27' 
        AND date_trunc('day', cs.day_) <= date_trunc('day',current_date())
        
) 

, addishop_base AS (
    SELECT
        day_ AS period,
        a.ally_slug,
        ac.ally_cluster,
        ac.ally_brand,
        MIN(a.start_date) AS shop_start_date,
        MIN(a.end_date) AS shop_end_date,
        CASE WHEN date_trunc('day', cs.day_) = date_trunc('day', MIN(a.start_date)) THEN 1 ELSE 0 END AS is_opt_in
    FROM {{ source('gold', 'dm_support_daily_calendar_series') }} cs
    LEFT JOIN {{ ref('dm_addishop_paying_allies_co') }} a
        ON cs.day_ >= date_trunc('day',a.start_date)
        AND cs.day_ <= date_trunc('day',a.end_date) 
    LEFT JOIN {{ ref('bl_ally_brand_ally_slug_status') }} ac
        ON a.ally_slug = ac.ally_slug
        AND ac.country_code = 'CO'
     WHERE 1=1
        AND a.ally_slug IS NOT NULL
        AND cs.day_ >= '2022-09-27' 
        AND date_trunc('day', cs.day_) <= date_trunc('day',current_date())
        AND lead_gen_fee::float > 0.0
      GROUP BY ALL
        
)
, addishop_base_unique AS (
    SELECT
        *,
        CASE WHEN date_trunc('day',shop_end_date) = date_trunc('day',period) 
            AND (COALESCE(LEAD(shop_end_date) OVER (PARTITION BY ally_slug ORDER by period),'1900-01-01') <> '2050-01-01'
            OR 
            COALESCE(LEAD(shop_start_date) OVER (PARTITION BY ally_slug ORDER by period),'1900-01-01') <> shop_start_date
            ) THEN 1 ELSE 0 END AS is_opt_out
    FROM addishop_base 
)
, shop_originations AS (
    SELECT
        date_trunc('day',origination_date_local) AS period,
        CASE WHEN  o.ally_slug <> pa_co.ally_slug THEN pa_co.ally_slug ELSE o.ally_slug END AS ally_slug,
        o.ally_cluster,
        SUM(gmv) AS gmv,
        SUM(gmv) FILTER (WHERE is_addishop_referral_paid IS TRUE) AS shop_paying_gmv,
        SUM(lead_gen_fee_amount) AS shop_revenue,
        COUNT(distinct application_id) FILTER (WHERE is_addishop_referral_paid IS TRUE) AS shop_paying_originations
    FROM {{ ref('dm_originations') }} o
    LEFT JOIN {{ ref('dm_addishop_paying_allies_co') }} AS pa_co 
      ON (o.ally_slug = pa_co.ally_slug OR array_contains(pa_co.grouped_allies, o.ally_slug) IS TRUE)
      AND o.origination_date >= pa_co.start_date
      AND o.origination_date <= pa_co.end_date
    WHERE 1=1
        AND country_code = 'CO'
    GROUP BY ALL
) 
, shop_origination_order AS (
  SELECT
    ally_slug,
    MIN(period) AS first_shop_origination_date,
    MAX(period) AS last_shop_origination_date
  FROM shop_originations
  WHERE 1=1
    AND COALESCE(shop_paying_originations,0) > 0
  GROUP BY 1
)

, agg_final aS (
SELECT
    ab.period AS date,
    ab.ally_slug,
    ab.ally_brand,
    ab.ally_cluster,
    ssc.ally_state,
    CASE WHEN das.ally_channel ILIKE '%COMM%' THEN 'E_COMMERCE' ELSE 'PAYLINK' END AS channel,
    CASE WHEN LOWER(ssc.ally_state) NOT IN ('disabled','rejected') THEN 1 ELSE 0 END AS is_able_originate,
    CASE WHEN ab.is_opt_out = 1 THEN 'OPT-OUT'
    WHEN ab.is_opt_in = 1 THEN 'OPT-IN'
     ELSE 'EXISTING' END AS shop_state,
    ab.shop_start_date,
    ab.shop_end_date,
    row_number() over (partition by ab.ally_slug order by ab.period) AS shop_day_number,
    so.gmv,
    so.gmv / f.price AS gmv_usd,
    so.shop_paying_gmv,
    so.shop_paying_gmv / f.price AS shop_paying_gmv_usd,
    so.shop_paying_originations,
    soo.first_shop_origination_date,
    soo.last_shop_origination_date,
    so.shop_revenue,
    so.shop_revenue / f.price AS shop_revenue_usd
FROM addishop_base_unique ab
LEFT JOIN state_status_calendar ssc
  ON ab.ally_slug = ssc.ally_slug
  AND ab.period = ssc.period
LEFT JOIN shop_originations so
  ON ab.ally_slug = so.ally_slug
  AND ab.period = so.period
LEFT JOIN shop_origination_order soo
  ON ab.ally_slug = soo.ally_slug
LEFT JOIN silver.d_fx_rate f
  ON f.is_active IS TRUE
  AND f.country_code = 'CO'
LEFT JOIN {{ ref('d_ally_slugs_co') }} das
  ON das.ally_slug = ab.ally_slug
)
SELECT
  date,
  ally_slug,
  ally_cluster,
  ally_state,
  channel,
  is_able_originate,
  shop_state,
  shop_start_date,
 CASE WHEN date_trunc('day',LEAD(shop_start_date) OVER (PARTITION BY ally_slug ORDER BY date)) = date_trunc('day',shop_start_date) THEN 
  LEAD(shop_end_date) OVER (PARTITION BY ally_slug ORDER BY date) ELSE shop_end_date END AS shop_end_date,
  shop_day_number,
  gmv,
  gmv_usd,
  shop_paying_gmv,
  shop_paying_gmv_usd,
  shop_paying_originations,
  first_shop_origination_date,
  last_shop_origination_date,
  shop_revenue,
  shop_revenue_usd,
  CASE WHEN row_number() over (partition by ally_slug order by date DESC) = 1 THEN 1 ELSE 0 END AS is_last_addishop_date,
  NOW() AS ingested_at,
  to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM agg_final 
WHERE 1=1
  AND date < current_date()
  AND ally_slug <> 'additest'