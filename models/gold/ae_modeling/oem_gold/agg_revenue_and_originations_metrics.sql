{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH
d_ally_management_allies AS (
    SELECT 'CO' AS country_code, ally_slug, ally_name  FROM {{ ref('d_ally_management_allies_co') }}
    UNION ALL
    SELECT 'BR' AS country_code, ally_slug, ally_name  FROM {{ ref('d_ally_management_allies_br') }}
)
,
dm_applications AS (
    SELECT *
    FROM {{ ref('dm_applications') }}
)
,
dm_originations_marketplace_suborders_co AS (
    SELECT *
    FROM {{ ref('dm_originations_marketplace_suborders_co') }}
)
,
aux_revenue_and_originations_stores_cities_regions_mapping AS (
    SELECT *
    FROM {{ ref('aux_revenue_and_originations_stores_cities_regions_mapping') }}
)
,
dm_originations_detailed_by_suborder AS (
    SELECT *
    FROM {{ ref('dm_originations_detailed_by_suborder') }}
)
,
fx_rates AS (
  SELECT
    SUM(price) FILTER(WHERE country_code='CO') AS fx_rate_cop,
    SUM(price) FILTER(WHERE country_code='BR') AS fx_rate_brl
  FROM {{ source('silver', 'd_fx_rate') }}
  WHERE is_active
)
,

/* + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - +*/
/* SECTION 1: GETTING APPLICATIONS AND ORIGINATIONS, BOTTH AFTER BEING DETAILED BY SUBORDER WHEN APPLICABLE */
/* + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - +*/
dm_applications_wo_refinance_detailed_by_suborder (
    -- No attribution as we only want to get N (N = # Suborders) rows per application id with the correct ally-relative fields
    -- Unlike originations which has its own attributed DM. Only bring fields used in the final dataset
    SELECT
        -- PK AND ROW TYPE
        MD5(COALESCE(mktplc_a.custom_application_suborder_pairing_id, a.application_id)) AS row_id,
        CASE WHEN mktplc_a.custom_application_suborder_pairing_id IS NOT NULL THEN 'MARKETPLACE_SUBORDER_APPLICATION' ELSE 'NON_MARKETPLACE_APPLICATION' END AS debug_row_granularity,
        -- A. KEY FIELDS
        a.country_code,
        a.application_id,
        mktplc_a.custom_application_suborder_pairing_id,
        a.client_id,
        a.application_datetime_local,
        COALESCE(mktplc_a.suborder_ally_slug, a.ally_slug) AS ally_slug,
        -- C. APPLICATIONS DATA
        COALESCE(mktplc_a.suborder_store_slug, a.store_slug) AS store_slug,
        a.application_datetime_local,
        a.client_type,
        a.application_channel,
        a.synthetic_channel,
        a.original_product,
        a.processed_product,
        a.synthetic_product_category,
        a.synthetic_product_subcategory,
        a.journey_name,
        -- E. ALLY DATA
        COALESCE(mktplc_a.suborder_ally_vertical, a.ally_vertical) AS ally_vertical,
        COALESCE(mktplc_a.suborder_ally_brand, a.ally_brand) AS ally_brand,
        COALESCE(mktplc_a.suborder_ally_cluster, a.ally_cluster) AS ally_cluster,
        -- I. OTHER FIELDS
        fx.fx_rate_cop,
        fx.fx_rate_brl,
        CASE WHEN a.processed_product = 'SANTANDER_CO' THEN 'SANTANDER' ELSE 'ADDI' END AS custom_application_with
    FROM      dm_applications                          AS a
    LEFT JOIN dm_originations_marketplace_suborders_co AS mktplc_a ON a.application_id = mktplc_a.application_id  -- 1:N relationship
    LEFT JOIN fx_rates                                 AS fx       ON TRUE
    WHERE a.application_channel != 'REFINANCE' -- Ignore refinance applications in all book metrics for now
)
,
pre_split_applications_and_originations_baseline AS (
    -- Amount fields for applications that didn't originate are set to NULL
    -- This dataset is the baseline to later split into the 3 key ones: APPLICATIONS, ORIGINATIONS, ORIGINATIONS_LOSSES
    SELECT
        -- UUID and debug fields
        a.country_code,
        a.row_id,
        a.application_id,
        a.custom_application_suborder_pairing_id,
        COALESCE(o.debug_row_granularity, a.debug_row_granularity) AS debug_row_granularity,
        -- TIMESTAMPS
        a.application_datetime_local,
        o.origination_date_local AS origination_datetime_local,
        -- FIELDS USED IN THE SPLIT
        o.gmv,
        o.requested_amount,
        o.guarantee_rate,
        o.guarantee_amount,
        o.approved_amount,
        o.approved_amount_filtered_for_losses,
        o.expected_final_losses,
        o.interest_rate,
        o.total_interest,
        o.consumer_revenue,
        o.lead_gen_fee_rate,
        o.lead_gen_fee_amount,
        o.ally_mdf,
        o.mdf_amount,
        o.synthetic_origination_marketplace_purchase_fee,
        o.synthetic_origination_marketplace_purchase_fee_amount,
        o.merchant_revenue,
        o.total_revenue,
        o.dq31_at_31_upb,
        o.dq31_at_31_opb,
        o.dq31_at_31_date,
        -- OTHER FIELDS NOT USED IN SPLIT -- FLAGS
        COALESCE(o.row_id IS NOT NULL, FALSE) AS is_origination,
        a.custom_application_with,
        COALESCE(o.total_interest > 0, FALSE) AS has_interest_flag,
        o.lbl,
        -- OTHER FIELDS NOT USED IN SPLIT -- DIMENSIONS AND FX
        a.client_id,
        a.ally_slug,
        o.loan_id,
        a.store_slug,
        a.client_type,
        o.term,
        a.application_channel,
        a.synthetic_channel,
        a.original_product,
        a.processed_product,
        a.synthetic_product_category,
        a.synthetic_product_subcategory,
        a.journey_name,
        a.ally_vertical,
        a.ally_brand,
        a.ally_cluster,
        a.fx_rate_cop,
        a.fx_rate_brl
    FROM       dm_applications_wo_refinance_detailed_by_suborder AS a
    LEFT JOIN  dm_originations_detailed_by_suborder              AS o ON a.row_id = o.row_id -- row_id represents a UUID that works on both marketplace and non-marketplace reliably
)
,

/* + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - +*/
/* SECTION 2: GETTING TIMESTAMPS FOR APPLICATIONS AND ORIGINATIONS BASED ON THE BASELINE DATASET FOR ALLY_SLUG, BRAND AND STORE */
/* + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - +*/
timestamps_ally_brand AS (
  SELECT
      country_code,
      ally_brand,
      MIN(application_datetime_local) AS min_ts_local_application_ally_brand,
      MIN(origination_datetime_local) AS min_ts_local_origination_ally_brand
  FROM pre_split_applications_and_originations_baseline
  GROUP BY 1,2
)
,
timestamps_ally_slug AS (
  SELECT
      country_code,
      ally_slug,
      MIN(application_datetime_local) AS min_ts_local_application_ally_slug,
      MIN(origination_datetime_local) AS min_ts_local_origination_ally_slug
  FROM pre_split_applications_and_originations_baseline
  GROUP BY 1,2
)
,
timestamps_store_slug AS (
  SELECT
      country_code,
      store_slug,
      MIN(application_datetime_local) AS min_ts_local_application_store_slug,
      MIN(origination_datetime_local) AS min_ts_local_origination_store_slug
  FROM pre_split_applications_and_originations_baseline
  GROUP BY 1,2
)
,

/* + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + -*/
/* SECTION 3: GETTING BASELINE DATASET (BASELINE) SPLIT INTO THE THREE INTEREST DATASETS UNDER SPECIFIC BUSINESS RULES                   */
/* + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + -*/
post_split_datasets AS (
    -- UNION ALL OF A., B. & C. All based in the previous CTE
    -- FILTER ONLY THE LAST 19 MONTHS (21 for losses, 19 for originations and applications)
    ( -- A. APPLICATIONS - AMOUNTS/RATE FIELDS ARE NULLIFIED IN THE PRE_SPLIT AND DQ3131 FIELDS ARE NULLIFIED IN THIS QUERY
    SELECT
        --UUIDS and debug
        'APPLICATIONS' AS dataset_type,
        country_code,
        row_id,
        -- REFERENCE TIMESTAMP
        application_datetime_local AS reference_datetime,
        -- SPLIT FIELDS: AMOUNTS RATES AND LOSSES FIELDS
        gmv,
        requested_amount,
        guarantee_rate,
        guarantee_amount,
        approved_amount,
        approved_amount_filtered_for_losses,
        expected_final_losses,
        interest_rate,
        total_interest,
        consumer_revenue,
        lead_gen_fee_rate,
        lead_gen_fee_amount,
        ally_mdf,
        mdf_amount,
        synthetic_origination_marketplace_purchase_fee,
        synthetic_origination_marketplace_purchase_fee_amount,
        merchant_revenue,
        total_revenue,
        NULL AS dq31_at_31_upb,
        NULL AS dq31_at_31_opb,
        NULL AS dq31_at_31_date
    FROM pre_split_applications_and_originations_baseline
    WHERE is_origination = FALSE
          AND application_datetime_local >= (DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '19 month')
    )
    UNION ALL
    (  -- B. ORIGINATIONS - NO MODIFICATIONS ON AMOUNTS/RATE FIELDS IN THE PRE_SPLIT AND DQ3131 FIELDS ARE NULLIFIED IN THIS QUERY
    SELECT
        --UUIDS and debug
        'ORIGINATIONS' AS dataset_type,
        country_code,
        row_id,
        -- REFERENCE TIMESTAMP
        origination_datetime_local AS reference_datetime,
        -- SPLIT FIELDS: AMOUNTS RATES AND LOSSES FIELDS
        gmv,
        requested_amount,
        guarantee_rate,
        guarantee_amount,
        approved_amount,
        approved_amount_filtered_for_losses,
        expected_final_losses,
        interest_rate,
        total_interest,
        consumer_revenue,
        lead_gen_fee_rate,
        lead_gen_fee_amount,
        ally_mdf,
        mdf_amount,
        synthetic_origination_marketplace_purchase_fee,
        synthetic_origination_marketplace_purchase_fee_amount,
        merchant_revenue,
        total_revenue,
        NULL AS dq31_at_31_upb,
        NULL AS dq31_at_31_opb,
        NULL AS dq31_at_31_date
    FROM pre_split_applications_and_originations_baseline
    WHERE is_origination = TRUE
          AND origination_datetime_local >= (DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '19 month')
    )
    UNION ALL
    ( -- C. ORIGINATIONS_LOSSES - AMOUNTS/RATE ARE NULLIFIED IN THIS QUERY (EXCEPT FOR ORIGINATION FLAGS), NO MODIFICATION TO DQ3131 FIELDS
    SELECT
        --UUIDS and debug
        'ORIGINATION_LOSSES' AS dataset_type,
        country_code,
        row_id,
        -- REFERENCE TIMESTAMP
        dq31_at_31_date::TIMESTAMP AS reference_datetime,
        -- SPLIT FIELDS: AMOUNTS RATES AND LOSSES FIELDS
        NULL AS gmv,
        NULL AS requested_amount,
        NULL AS guarantee_rate,
        NULL AS guarantee_amount,
        NULL AS approved_amount,
        NULL AS approved_amount_filtered_for_losses,
        NULL AS expected_final_losses,
        NULL AS interest_rate,
        NULL AS total_interest,
        NULL AS consumer_revenue,
        NULL AS lead_gen_fee_rate,
        NULL AS lead_gen_fee_amount,
        NULL AS ally_mdf,
        NULL AS mdf_amount,
        NULL AS synthetic_origination_marketplace_purchase_fee,
        NULL AS synthetic_origination_marketplace_purchase_fee_amount,
        NULL AS merchant_revenue,
        NULL AS total_revenue,
        dq31_at_31_upb,
        dq31_at_31_opb,
        dq31_at_31_date
    FROM pre_split_applications_and_originations_baseline
    WHERE is_origination = TRUE
          AND dq31_at_31_date <  CURRENT_DATE()
          AND dq31_at_31_date >= (DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '21 month')
    )
)

/* + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + -*/
/* SECTION 4: SELECT SECTION 3 DATASET POPULATED WITH ALL FIELDS FROM OTHER SOURCES, FIELDS THAT ARE NOT SUBJECT OF THE `dataset_type`    */
/* + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + -*/
SELECT
    --UUIDS and debug
    psd.dataset_type,
    psd.country_code,
    psd.row_id,
    b.application_id,
    b.custom_application_suborder_pairing_id,
    b.client_id,
    b.ally_slug,
    b.store_slug,
    b.loan_id,
    b.debug_row_granularity,
    -- REFERENCE TIMESTAMP + OTHERS
    psd.reference_datetime,
    b.origination_datetime_local,
    b.application_datetime_local,
    -- AMOUNTS, RATES AND LOSSES FIELDS
    psd.gmv,
    psd.requested_amount,
    psd.guarantee_rate,
    psd.guarantee_amount,
    psd.approved_amount,
    psd.approved_amount_filtered_for_losses,
    psd.expected_final_losses, -- This column name was changed to match the concept in the `agg_originations_metrics`
    psd.interest_rate,
    psd.total_interest,
    psd.consumer_revenue,
    psd.lead_gen_fee_rate,
    psd.lead_gen_fee_amount,
    psd.ally_mdf,
    psd.mdf_amount,
    psd.merchant_revenue,
    psd.synthetic_origination_marketplace_purchase_fee,
    psd.synthetic_origination_marketplace_purchase_fee_amount,
    psd.total_revenue,
    psd.dq31_at_31_upb,
    psd.dq31_at_31_opb,
    psd.dq31_at_31_date, -- This column name was changed to match the concept in the `agg_originations_metrics`
    -- OTHER DATASETS COLUMNS - as they are. Only brought here to avoid redundancy in SECTION 3
    -- ... FLAGS
    b.custom_application_with AS application_with,
    b.has_interest_flag,
    b.lbl,
    -- ... DIMENSION, RATES, STORE LOCATION AND TIMESTAMPS
    am_a.ally_name,
    b.client_type,
    b.application_channel,
    b.term,
    b.synthetic_channel,
    b.original_product,
    b.processed_product,
    b.synthetic_product_category,
    b.synthetic_product_subcategory,
    b.journey_name,
    b.ally_vertical,
    b.ally_brand,
    b.ally_cluster,
    b.fx_rate_cop,
    b.fx_rate_brl,
    scr.city_name,
    scr.region_name AS state_name,
    ab.min_ts_local_application_ally_brand,
    ab.min_ts_local_origination_ally_brand,
    als.min_ts_local_origination_ally_slug,
    als.min_ts_local_application_ally_slug,
    ss.min_ts_local_application_store_slug,
    ss.min_ts_local_origination_store_slug,
    -- OTHER DATASETS COLUMNS - Calculated. Only brought here to avoid redundancy in SECTION 3
    DATEDIFF(b.application_datetime_local::DATE,ab.min_ts_local_application_ally_brand::DATE) AS days_since_first_application_ally_brand,
    DATEDIFF(b.application_datetime_local::DATE,als.min_ts_local_application_ally_slug::DATE) AS days_since_first_application_ally_slug,
    DATEDIFF(b.application_datetime_local::DATE,ss.min_ts_local_application_store_slug::DATE) AS days_since_first_application_store_slug,
    DATEDIFF(COALESCE(b.origination_datetime_local::DATE,b.application_datetime_local::DATE), ab.min_ts_local_origination_ally_brand::DATE) AS days_since_first_origination_ally_brand,
    DATEDIFF(COALESCE(b.origination_datetime_local::DATE,b.application_datetime_local::DATE), als.min_ts_local_origination_ally_slug::DATE) AS days_since_first_origination_ally_slug,
    DATEDIFF(COALESCE(b.origination_datetime_local::DATE,b.application_datetime_local::DATE), ss.min_ts_local_origination_store_slug::DATE) AS days_since_first_origination_store_slug,
    -- DATA PLATFORM COLUMNS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM      post_split_datasets                                         AS psd
LEFT JOIN pre_split_applications_and_originations_baseline            AS b    ON psd.row_id = b.row_id
LEFT JOIN d_ally_management_allies                                    AS am_a ON b.ally_slug = am_a.ally_slug  AND b.country_code = am_a.country_code
LEFT JOIN aux_revenue_and_originations_stores_cities_regions_mapping  AS scr  ON b.store_slug = scr.store_slug AND b.country_code = scr.country_code
LEFT JOIN timestamps_ally_brand                                       AS ab   ON b.ally_brand = ab.ally_brand  AND b.country_code = ab.country_code
LEFT JOIN timestamps_ally_slug                                        AS als  ON b.ally_slug = als.ally_slug   AND b.country_code = als.country_code
LEFT JOIN timestamps_store_slug                                       AS ss   ON b.store_slug = ss.store_slug  AND b.country_code = ss.country_code
