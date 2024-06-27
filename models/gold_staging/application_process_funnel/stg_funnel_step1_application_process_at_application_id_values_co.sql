{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH
f_applications_co_filtered AS (
    SELECT *
    FROM {{ ref('f_applications_co') }}
    WHERE application_id IS NOT NULL
)
,
f_originations_bnpl_co AS (
    SELECT *
    FROM {{ ref('f_originations_bnpl_co') }}
)
,
dm_applications_co_reference_data AS (
    SELECT
        MAX(application_datetime) AS processing_reference_timestamp,
        MAX(application_datetime) - interval '5' MINUTE AS processing_reference_timestamp_with_slack
    FROM {{ ref('dm_applications') }}
    WHERE country_code = 'CO'
)
,
bl_application_preapproval_proxy AS (
    SELECT
        *
    FROM {{ ref('bl_application_preapproval_proxy') }}
)
,
f_origination_events_co_logs_filtered AS (
    SELECT *
    FROM {{ ref('f_origination_events_co_logs') }}
    WHERE application_id IS NOT  NULL
)
,
bl_application_product_co AS (
    SELECT *
    FROM {{ ref('bl_application_product_co') }}
)
,
f_applications_marketplace_suborders_co AS (
    SELECT *
    FROM {{ ref('f_applications_marketplace_suborders_co') }}
)
,
bl_application_preapproval_proxy_co AS (
    SELECT *
    FROM {{ ref('bl_application_preapproval_proxy') }}
    WHERE country_code = 'CO'
)
,
bl_ally_brand_ally_slug_status_co AS (
    SELECT *
    FROM {{ ref('bl_ally_brand_ally_slug_status') }}
    WHERE country_code = 'CO'
)
,
aux_funnel_application_journey_grouping_co AS (
    SELECT *
    FROM {{ ref('aux_funnel_application_journey_grouping') }}
    WHERE country_code = 'CO'
)
,
marketplace_applications_ally_extra_arrays AS (
    -- Getting unique values for all suborders of an application
    SELECT
        mktp_so.application_id,
        COLLECT_SET(bs.ally_cluster) AS suborders_ally_cluster_array,
        COLLECT_SET(bs.ally_brand) AS suborders_ally_brand_array,
        COLLECT_SET(bs.ally_vertical) AS suborders_ally_vertical_array
    FROM      f_applications_marketplace_suborders_co AS mktp_so
    LEFT JOIN bl_ally_brand_ally_slug_status_co       AS bs      ON bs.ally_slug = mktp_so.suborder_ally_slug
    GROUP BY 1
)
,
f_origination_events_co_logs_backfill_and_timestamps AS (
    -- STEP 2: Calculate the synthetic_ocurred_on_local
    SELECT
        *,
        COALESCE(approval_ocurred_on_local, minimum_ocurred_on_local) AS synthetic_ocurred_on_local
    FROM
    ( -- STEP 1: Backfill some fields and calculate flags and key timestamps per application
        SELECT
            application_id,
            -- BACKFILLED FIELDS
            FIRST_VALUE(ally_slug,   TRUE) AS ally_slug,
            FIRST_VALUE(channel,     TRUE) AS channel,
            FIRST_VALUE(client_id,   TRUE) AS client_id,
            FIRST_VALUE(client_type, TRUE) AS client_type,
            FIRST_VALUE(journey_name,TRUE) AS journey_name,
            -- CALCULATED TIMESTAMP AND FLAG FIELDS
            ARRAYS_OVERLAP( COLLECT_SET(event_type), ARRAY('DECLINATION','ABANDONMENT','REJECTION','APPROVAL')) AS has_finished,
            ARRAY_CONTAINS( COLLECT_SET(event_type), 'APPROVAL') AS has_reached_approval,
            --MAX(COALESCE(event_type = 'APPROVAL', FALSE)) AS has_reached_approval,
            MIN(                                       from_utc_timestamp(ocurred_on,'America/Bogota')    ) AS minimum_ocurred_on_local,
            MAX(CASE WHEN event_type = 'APPROVAL' THEN from_utc_timestamp(ocurred_on,'America/Bogota') END) AS approval_ocurred_on_local
        FROM f_origination_events_co_logs_filtered
        GROUP BY 1
    ) -- STEP 1 --
-- STEP 2 --
)
,
synthetic_journey_name_for_funnel AS (
    SELECT
        DISTINCT application_id,
        'PROSPECT_FINANCIA_SANTANDER_ADDI_CO' AS custom_journey_name
    FROM f_origination_events_co_logs_filtered
    WHERE journey_name = 'PROSPECT_FINANCIA_SANTANDER_CO' AND journey_stage_name IN ('underwriting-co')
)

-- FINAL DATASET -- ALL APPLICATIONS ARE CONSIDERED (FOR FINISHED ONES USE:has_finished), AFTER BACKFILLING AND ADDITIONAL LOGIC ON SOME FIELDS
-- STEP 3: Calculate the application_process_baseline_id: a date + a client + a journey + an ally slug list (1 or more)
SELECT
    *, --To avoid redundancy, only new fields
    -- application_process_baseline_id (new) -- KEY UUID: In a day (local), for a client, buying to an ally (or an ally combination) during a journey
    MD5(CONCAT(
        synthetic_ocurred_on_date_local::STRING,
        client_id,
        ally_slugs_string,
        synthetic_journey_name
        )
    ) AS application_process_baseline_id,
    -- DATA PLATFORM COLUMNS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM
( -- STEP 2: Convert marketplace suborders_ally_slug_array into a string, for non-marketplace orders just get the ally_slug
    SELECT
        *, --To avoid redundancy, only new fields
        -- NEW -- ALLIES/CLUSTERS/BRANDS/VERTICALS AS STRING , mixing both marketplace (as string) and non-marketplace
        CASE
            WHEN suborders_ally_slug_array IS NOT NULL THEN CONCAT_WS(',', suborders_ally_slug_array)
            ELSE ally_slug
        END AS ally_slugs_string,
        CASE
            WHEN suborders_ally_cluster_array IS NOT NULL THEN CONCAT_WS(',', suborders_ally_cluster_array)
            ELSE ally_cluster
        END AS ally_clusters_string,
        CASE
            WHEN suborders_ally_brand_array IS NOT NULL THEN CONCAT_WS(',', suborders_ally_brand_array)
            ELSE ally_brand
        END AS ally_brands_string,
        CASE
            WHEN suborders_ally_vertical_array IS NOT NULL THEN CONCAT_WS(',', suborders_ally_vertical_array)
            ELSE ally_vertical
        END AS ally_verticals_string
    FROM
    ( -- STEP 1: Backfill some fields, custom logic on others and columns from other sources
        SELECT
            -- BACKFILLED FIELDS
            COALESCE(a.application_id, abt.application_id) AS application_id,
            COALESCE(a.ally_slug, abt.ally_slug) AS ally_slug,
            COALESCE(a.channel, abt.channel, a.application_channel_legacy) AS channel,
            COALESCE(a.client_id, abt.client_id) AS client_id,
            CASE WHEN COALESCE(a.client_type, abt.client_type) = 'LEAD' THEN 'PROSPECT' ELSE COALESCE(a.client_type, abt.client_type) END AS client_type,
            COALESCE(a.journey_name, abt.journey_name) AS original_journey_name,
            -- CUSTOM JOURNEY NAME FIELDS
            COALESCE(sjn.custom_journey_name, a.journey_name, abt.journey_name) AS synthetic_journey_name,
            jg.journey_homolog,
            -- CALCULATED TIMESTAMP AND FLAG FIELDS
            COALESCE(abt.has_finished, FALSE) AS has_finished,
            COALESCE(abt.has_reached_approval, FALSE) AS has_reached_approval,
            abt.minimum_ocurred_on_local,
            abt.approval_ocurred_on_local,
            abt.synthetic_ocurred_on_local,
            abt.synthetic_ocurred_on_local::DATE AS synthetic_ocurred_on_date_local,
            -- MARKETPLACE FIELDS
            CASE WHEN a.suborders_ally_slug_array       IS NOT NULL THEN IF(SIZE(a.suborders_ally_slug_array)>0,       SORT_ARRAY(a.suborders_ally_slug_array),NULL)       END AS suborders_ally_slug_array,
            CASE WHEN aea.suborders_ally_cluster_array  IS NOT NULL THEN IF(SIZE(aea.suborders_ally_cluster_array)>0,  SORT_ARRAY(aea.suborders_ally_cluster_array),NULL)  END AS suborders_ally_cluster_array,
            CASE WHEN aea.suborders_ally_brand_array    IS NOT NULL THEN IF(SIZE(aea.suborders_ally_brand_array)>0,    SORT_ARRAY(aea.suborders_ally_brand_array),NULL)    END AS suborders_ally_brand_array,
            CASE WHEN aea.suborders_ally_vertical_array IS NOT NULL THEN IF(SIZE(aea.suborders_ally_vertical_array)>0, SORT_ARRAY(aea.suborders_ally_vertical_array),NULL) END AS suborders_ally_vertical_array,
            -- BNPL ORIGINATION FIELDS
            o.term,
            -- ALLY RELATED VARIABLES
            abas.ally_cluster, -- with same default
            abas.ally_brand,
            abas.ally_vertical,
            -- PREAPPROVAL CUSTOM PROXY
            COALESCE(pp.is_using_preapproval_proxy, FALSE) AS is_using_preapproval_proxy,  -- flag_preapproval
            -- APPLICATION PRODUCT
            ap.original_product,
            ap.synthetic_product_category,
            ap.synthetic_product_subcategory,
            -- DEBUGGING TIMESTAMP
            from_utc_timestamp(a.application_date,'America/Bogota') AS debug_application_datetime_local
        FROM        f_applications_co_filtered                            AS a
        INNER JOIN  f_origination_events_co_logs_backfill_and_timestamps  AS abt  ON a.application_id = abt.application_id -- applications backfill and timestamps
        LEFT JOIN   f_originations_bnpl_co                                AS o    ON a.application_id = o.application_id
        LEFT JOIN   synthetic_journey_name_for_funnel                     AS sjn  ON a.application_id = sjn.application_id
        LEFT JOIN   aux_funnel_application_journey_grouping_co            AS jg   ON COALESCE(sjn.custom_journey_name, a.journey_name, abt.journey_name) = jg.synthetic_journey_name
        LEFT JOIN   bl_ally_brand_ally_slug_status_co                     AS abas ON COALESCE(a.ally_slug, abt.ally_slug) = abas.ally_slug -- country prefiltered already
        LEFT JOIN   bl_application_preapproval_proxy_co                   AS pp   ON a.application_id = pp.application_id
        LEFT JOIN   bl_application_product_co                             AS ap   ON a.application_id = ap.application_id
        LEFT JOIN   marketplace_applications_ally_extra_arrays            AS aea  ON a.application_id = aea.application_id
        LEFT JOIN   dm_applications_co_reference_data                     AS ard  ON TRUE
        -- This where is to make sure to get only data from applications that have already been processed
        -- in the `ae_origination_events_gold` DAG (dm_applications being one of the first to run),
        -- so that the BLs bring non-null data
        -- WHERE a.application_date <= ard.processing_reference_timestamp_with_slack -- Testing data lag without condition
    ) -- STEP 1 --
)  -- STEP 2 --
-- STEP 3 --