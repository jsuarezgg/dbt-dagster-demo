{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH orig_data AS (
    SELECT
        store_slug,
        MIN(origination_date_local::DATE) AS min_origination_date_local,
        MAX(origination_date_local::DATE) AS max_origination_date_local,
        COUNT(DISTINCT application_id) FILTER (WHERE origination_date_local::DATE >= DATEADD(FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Bogota')::DATE, -7)        ) AS num_originations_last_7_days,
        COUNT(DISTINCT application_id) FILTER (WHERE origination_date_local::DATE >= DATEADD(FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Bogota')::DATE, -30)       ) AS num_originations_last_30_days,
        COUNT(DISTINCT application_id) FILTER (WHERE origination_date_local::DATE >= DATE_TRUNC('month', FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Bogota'))::DATE) AS num_originations_current_month,
        SUM(CASE WHEN origination_date_local::DATE >= DATE_TRUNC('month', FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Bogota'))::DATE THEN gmv END) AS gmv_current_month
    FROM {{ ref('dm_originations_detailed_by_suborder') }}
    WHERE country_code = 'CO'
        AND store_slug IS NOT NULL
    GROUP BY 1
)
,
apps_data AS (
    SELECT
        COALESCE(mktplc_a.suborder_store_slug, a.store_slug) AS store_slug,
        MIN(a.application_datetime_local::DATE) AS min_application_date_local,
        MAX(a.application_datetime_local::DATE) AS max_application_date_local,
        COUNT(DISTINCT a.application_id) FILTER (WHERE a.application_datetime_local::DATE >= DATEADD(FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Bogota')::DATE, -7)        ) AS num_applications_last_7_days,
        COUNT(DISTINCT a.application_id) FILTER (WHERE a.application_datetime_local::DATE >= DATEADD(FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Bogota')::DATE, -30)       ) AS num_applications_last_30_days,
        COUNT(DISTINCT a.application_id) FILTER (WHERE a.application_datetime_local::DATE >= DATE_TRUNC('month', FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Bogota'))::DATE) AS num_applications_current_month
    FROM      {{ ref('dm_applications') }}                          AS a
    LEFT JOIN {{ ref('dm_applications_marketplace_suborders_co') }} AS mktplc_a ON a.application_id = mktplc_a.application_id  -- 1:N relationship
    WHERE   a.country_code = 'CO'
        AND a.store_slug IS NOT NULL
        AND a.application_channel != 'REFINANCE' -- Ignore refinance applications in all book metrics for now
  GROUP BY 1
)
SELECT
  a.store_slug,
  a.min_application_date_local,
  a.max_application_date_local,
  o.min_origination_date_local,
  o.max_origination_date_local,
  a.num_applications_last_7_days,
  a.num_applications_last_30_days,
  a.num_applications_current_month,
  COALESCE(o.num_originations_last_7_days, 0) AS num_originations_last_7_days,
  COALESCE(o.num_originations_last_30_days, 0) AS num_originations_last_30_days,
  COALESCE(o.num_originations_current_month, 0) AS num_originations_current_month,
  COALESCE(o.gmv_current_month, 0) AS gmv_current_month
FROM apps_data AS a
LEFT JOIN orig_data AS o ON o.store_slug = a.store_slug