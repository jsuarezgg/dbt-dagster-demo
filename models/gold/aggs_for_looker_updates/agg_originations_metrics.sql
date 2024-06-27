{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH
dm_originations_detailed_by_suborder AS (
    -- V2: Context - Carlos
    SELECT *
    FROM {{ ref('dm_originations_detailed_by_suborder') }}
)
,
dm_ally_slug_co AS (
    -- V1+V2: Context - Julio & Carlos
    SELECT *
    FROM {{ ref('dm_ally_slug_co') }}
)
,
agg_shop_ally_metrics_co_filtered AS (
    -- V1: Context - Alex & Julio
    SELECT DISTINCT ally_slug
    FROM {{ ref('agg_shop_ally_metrics_co') }}
    WHERE shop_state IN ('OPT-IN','EXISTING') AND `date` < DATE_TRUNC('year', CURRENT_DATE() - INTERVAL '1 day')
)
,
funding_manual_metrics AS (
    -- V1: Context - Alexis & Julio
    SELECT
        a.country_code,
        DATE_TRUNC('month', a.date_) AS period,
        CASE
            WHEN a.metric ILIKE ('%CUPO%') THEN 'CUPO'
            WHEN a.metric ILIKE ('%GRANDE%') THEN 'GRANDE'
            WHEN a.metric ILIKE ('%INTRO%') THEN 'INTRO'
        END AS synthetic_product_category,
        MAX(a.value_) FILTER (WHERE a.metric IN ('CUPO CTS + U/W', 'INTRO CTS + U/W', 'GRANDE CTS + U/W')) AS cts_uw,
        MAX(a.value_) FILTER (WHERE a.metric IN ('CoF INTRO', 'CoF CUPO', 'CoF GRANDE')) AS cof,
        MAX(b.total_cts_uw) AS total_cts_uw,
        MAX(b.total_cof) AS total_cof
    FROM {{ source('gold', 'agg_addi_management_manual_metrics_inputs') }} AS a
    LEFT JOIN (
        SELECT
            country_code,
            DATE_TRUNC('month', date_) AS period,
            MAX(value_) FILTER (WHERE metric = 'TOTAL CTS + U/W') AS total_cts_uw,
            MAX(value_) FILTER (WHERE metric = 'CoF TOTAL') AS total_cof
        FROM {{ source('gold', 'agg_addi_management_manual_metrics_inputs') }}
        WHERE metric IN ('TOTAL CTS + U/W', 'CoF TOTAL')
        GROUP BY 1,2
    ) AS b ON b.period = DATE_TRUNC('month', a.date_)
           AND b.country_code = a.country_code
    WHERE a.metric IN ( 'CUPO CTS + U/W', 'INTRO CTS + U/W', 'GRANDE CTS + U/W', 'CoF INTRO', 'CoF CUPO', 'CoF GRANDE')
    GROUP BY 1,2,3
)
, funding_manual_metrics_txn AS (
    -- V1: Context - Alexis & Julio
    SELECT
        country_code,
        metric_,
        scope_,
        category_,
        window_,
        date_,
        CAST(term_ AS DOUBLE) AS term_,
        CAST(value_ AS DOUBLE) AS value_
    FROM {{ source('gold', 'agg_addi_management_manual_metrics_inputs_txn') }}
)
,
originations_baseline AS (
    -- V1+V2: Context - Alexis & Julio & Carlos
    SELECT
        -- A. Sources keys
        o.row_id,
        o.application_id,
        o.custom_application_suborder_pairing_id,
        o.loan_id,
        -- B. Sources values
        o.country_code,
        o.origination_date_local AS origination_datetime_local,
        o.origination_date_local::DATE AS origination_date_local,
        o.term,
        o.client_type,
        o.application_channel,
        o.synthetic_channel,
        o.processed_product,
        o.synthetic_product_category,
        o.synthetic_product_subcategory,
        o.is_addishop_referral,
        o.is_addishop_referral_paid,
        o.ally_slug,
        o.store_slug,
        o.ally_vertical,
        o.ally_brand,
        o.ally_cluster,
        o.guarantee_provider_with_default AS guarantee_provider,
        o.segment,
        o.santander_origination AS is_santander,
        o.total_interest,
        o.guarantee_amount,
        o.gross_expected_final_losses_amount,
        o.guarantee_expected_loss_recovery_amount,
        o.fng_cost_amount,
        o.lead_gen_fee_amount,
        o.mdf_amount,
        o.expected_collection_fee_amount,
        o.gmv,
        o.approved_amount,
        o.approved_amount_filtered_for_losses,
        o.synthetic_origination_marketplace_purchase_fee_amount,
        o.consumer_revenue,
        o.merchant_revenue,
        o.total_revenue,
        o.expected_final_losses,
        o.fx_rate,
        o.has_fga_flag,
        o.dq31_at_31_upb,
        o.dq31_at_31_opb,
        o.dq31_at_31_date,
        o.debug_attribution_weight AS suborder_attribution_weight,
        -- C. External sources values
        als.min_origination_date_local AS first_slug_origination_date,
        -- -- All metrics in C: Multiplied by attribution weight to avoid duplication of these `value_` for marketplace multi-seller applications
        COALESCE( pc.value_, 0) AS payment_cost,
        COALESCE( cts.value_, 0) AS cost_to_serve,
        COALESCE( uw.value_, 0) AS underwriting_cost,
        COALESCE( cof_grande.value_, 0) AS cost_of_funding_grande,
        COALESCE( cof_cupo.value_, 0) AS cost_of_funding_cupo,
        -- D. Custom calculations
        -- -- First metric in D: Multiplied by attribution weight to avoid duplication of these `value_` for marketplace multi-seller applications
        CASE
            WHEN o.synthetic_product_subcategory IN ('GRANDE', 'CUPO Flex', 'INTRO') THEN COALESCE( wal_ib.value_, 0)
            ELSE COALESCE(wal_if.value_, 0)
        END AS wal,
        CASE
            WHEN o.country_code = 'CO' AND sa.ally_slug IS NOT NULL THEN TRUE
            WHEN o.country_code = 'CO' AND sa.ally_slug IS NULL     THEN FALSE
            ELSE NULL
        END AS is_existing_shop_ally_before_current_year,
        CASE
            WHEN ROW_NUMBER() OVER (PARTITION BY o.country_code, o.ally_slug ORDER BY o.origination_date_local) = 1 THEN 1 --ORDER BY timestsamp
            ELSE 0
        END AS is_first_slug_origination
    --- V1 + V2: Context - Alexis & Julio & Carlos
    FROM      dm_originations_detailed_by_suborder AS o
    LEFT JOIN dm_ally_slug_co                      AS als ON als.ally_slug = o.ally_slug AND o.country_code = 'CO'
    LEFT JOIN agg_shop_ally_metrics_co_filtered    AS sa  ON sa.ally_slug  = o.ally_slug AND o.country_code = 'CO'
    --- Additional Joins -- V1: Context - Alexis & Julio
    LEFT JOIN funding_manual_metrics_txn AS pc         ON o.country_code = pc.country_code
                                                          AND DATE_TRUNC('month', o.origination_date_local)::DATE = pc.date_
                                                          AND pc.metric_ = 'payment_cost'
    LEFT JOIN funding_manual_metrics_txn AS cts        ON o.country_code = cts.country_code
                                                          AND DATE_TRUNC('month', o.origination_date_local)::DATE = cts.date_
                                                          AND cts.metric_ = 'CTS'
    LEFT JOIN funding_manual_metrics_txn AS uw         ON o.country_code = uw.country_code
                                                          AND DATE_TRUNC('month', o.origination_date_local)::DATE = uw.date_
                                                          AND o.synthetic_product_category = uw.scope_
                                                          AND uw.metric_ = 'UW'
    LEFT JOIN funding_manual_metrics_txn AS cof_grande ON o.country_code = cof_grande.country_code
                                                          AND DATE_TRUNC('month', o.origination_date_local)::DATE = cof_grande.date_
                                                          AND ROUND(o.term * 0.9, 1) = ROUND(cof_grande.term_, 1)
                                                          AND o.synthetic_product_category = cof_grande.scope_
                                                          AND cof_grande.metric_ = 'COF'
                                                          AND cof_grande.scope_= 'GRANDE'
    LEFT JOIN funding_manual_metrics_txn AS cof_cupo   ON o.country_code = cof_cupo.country_code
                                                          AND DATE_TRUNC('month', o.origination_date_local)::DATE = cof_cupo.date_
                                                          AND o.term  = cof_cupo.term_
                                                          AND o.synthetic_product_category = cof_cupo.scope_
                                                          AND cof_cupo.metric_ = 'COF'
                                                          AND cof_cupo.scope_ IN ('INTRO', 'CUPO')
    LEFT JOIN funding_manual_metrics_txn AS wal_if     ON o.country_code = wal_if.country_code
                                                          AND DATE_TRUNC('year', o.origination_date_local)::DATE = wal_if.date_
                                                          AND o.term = wal_if.term_
                                                          AND wal_if.metric_ = 'WAL'
                                                          AND wal_if.scope_ = 'interest_free'
    LEFT JOIN funding_manual_metrics_txn AS wal_ib     ON o.country_code = wal_ib.country_code
                                                          AND DATE_TRUNC('year', o.origination_date_local)::DATE = wal_ib.date_
                                                          AND o.term = wal_ib.term_
                                                          AND wal_ib.metric_ = 'WAL'
                                                          AND wal_ib.scope_ = 'interest_bearing'
    WHERE o.application_channel != 'REFINANCE' -- Ignore refinance applications in all book metrics for now
)
,
origination_date_metrics AS (
    -- V1: Context - Alexis & Julio
    -- Low granularity dataset: aggregating originations based on the origination_date_local with some WHERE conditions applied
    SELECT
        country_code,
        origination_date_local AS period_date,
        term,
        client_type,
        application_channel,
        synthetic_channel,
        processed_product,
        synthetic_product_category,
        synthetic_product_subcategory,
        is_addishop_referral,
        is_addishop_referral_paid,
        ally_slug,
        store_slug,
        ally_vertical,
        ally_brand,
        ally_cluster,
        guarantee_provider,
        segment,
        is_santander,
        has_fga_flag,
        first_slug_origination_date,
        SUM(total_interest) AS total_interest,
        SUM(guarantee_amount) AS guarantee_amount,
        SUM(gross_expected_final_losses_amount) AS gross_expected_final_losses_amount,
        SUM(guarantee_expected_loss_recovery_amount) AS guarantee_expected_loss_recovery_amount,
        SUM(fng_cost_amount) AS fng_cost_amount,
        SUM(lead_gen_fee_amount) AS lead_gen_fee_amount,
        SUM(mdf_amount) AS mdf_amount,
        SUM(expected_collection_fee_amount) AS expected_collection_fee_amount,
        SUM(gmv) AS gmv,
        SUM(approved_amount) AS approved_amount,
        SUM(approved_amount_filtered_for_losses) AS approved_amount_filtered_for_losses,
        SUM(synthetic_origination_marketplace_purchase_fee_amount) AS synthetic_origination_marketplace_purchase_fee_amount,
        SUM(consumer_revenue) AS consumer_revenue,
        SUM(merchant_revenue) AS merchant_revenue,
        SUM(total_revenue) AS total_revenue,
        SUM(total_interest/fx_rate) AS total_interest_usd,
        SUM(guarantee_amount/fx_rate) AS guarantee_amount_usd,
        SUM(gross_expected_final_losses_amount/fx_rate) AS gross_expected_final_losses_amount_usd,
        SUM(guarantee_expected_loss_recovery_amount/fx_rate) AS guarantee_expected_loss_recovery_amount_usd,
        SUM(fng_cost_amount/fx_rate) AS fng_cost_amount_usd,
        SUM(lead_gen_fee_amount/fx_rate) AS lead_gen_fee_amount_usd,
        SUM(mdf_amount/fx_rate) AS mdf_amount_usd,
        SUM(expected_collection_fee_amount/fx_rate) AS expected_collection_fee_amount_usd,
        SUM(gmv/fx_rate) AS gmv_usd,
        SUM(approved_amount/fx_rate) AS approved_amount_usd,
        SUM(consumer_revenue/fx_rate) AS consumer_revenue_usd,
        SUM(merchant_revenue/fx_rate) AS merchant_revenue_usd,
        SUM(total_revenue/fx_rate) AS total_revenue_usd,
        SUM(expected_final_losses) AS expected_final_losses,
        MAX(is_first_slug_origination) AS is_first_slug_origination,
        MAX(is_existing_shop_ally_before_current_year) AS is_existing_shop_ally_before_current_year,
        --  Context: V1 - Julio & Alexis - calculations for manual inputs at the application-origination level
        --           V2 - Carlos - attribution for each suborder. The calculations below: `cost_of_funding_usd`,
        --                `numerator_weighted_wal_usd` & `numerator_weighted_term_usd` do not require attribution
        --                as they include `gmv` in the calculation, and it already considers the attribution factor
        --                (check `dm_originations_detailed_by_suborder` for further details)
        SUM(COALESCE(suborder_attribution_weight, 1) * payment_cost / fx_rate) AS payment_cost_usd,
        SUM(COALESCE(suborder_attribution_weight, 1) * cost_to_serve / fx_rate) AS cost_to_serve_usd,
        SUM(COALESCE(suborder_attribution_weight, 1) * underwriting_cost / fx_rate) AS underwriting_cost_usd,
        SUM(COALESCE(suborder_attribution_weight, 1) * wal) AS numerator_wal,
        SUM((cost_of_funding_grande + cost_of_funding_cupo) * gmv / fx_rate) AS cost_of_funding_usd,
        SUM(wal * gmv / fx_rate) AS numerator_weighted_wal_usd,
        SUM(term * gmv / fx_rate) AS numerator_weighted_term_usd,
        -- End of context
        SUM(synthetic_origination_marketplace_purchase_fee_amount/ fx_rate) AS synthetic_origination_marketplace_purchase_fee_amount_usd,
        NULL AS dq31_at_31_upb,
        NULL AS dq31_at_31_opb
    FROM  originations_baseline
    WHERE origination_date_local < CURRENT_DATE()
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
)
,
losses_date_metrics AS (
    -- V1: Context - Alexis & Julio
    -- Low granularity dataset: aggregating losses based on the dq31_at_31_date with some WHERE conditions applied
    SELECT
        country_code,
        dq31_at_31_date AS period_date,
        term,
        client_type,
        application_channel,
        synthetic_channel,
        processed_product,
        synthetic_product_category,
        synthetic_product_subcategory,
        is_addishop_referral,
        is_addishop_referral_paid,
        ally_slug,
        store_slug,
        ally_vertical,
        ally_brand,
        ally_cluster,
        guarantee_provider,
        segment,
        is_santander,
        has_fga_flag,
        first_slug_origination_date,
        NULL AS total_interest,
        NULL AS guarantee_amount,
        NULL AS gross_expected_final_losses_amount,
        NULL AS guarantee_expected_loss_recovery_amount,
        NULL AS fng_cost_amount,
        NULL AS lead_gen_fee_amount,
        NULL AS mdf_amount,
        NULL AS expected_collection_fee_amount,
        NULL AS gmv,
        NULL AS approved_amount,
        NULL AS approved_amount_filtered_for_losses,
        NULL AS synthetic_origination_marketplace_purchase_fee_amount,
        NULL AS consumer_revenue,
        NULL AS merchant_revenue,
        NULL AS total_revenue,
        NULL AS total_interest_usd,
        NULL AS guarantee_amount_usd,
        NULL AS gross_expected_final_losses_amount_usd,
        NULL AS guarantee_expected_loss_recovery_amount_usd,
        NULL AS fng_cost_amount_usd,
        NULL AS lead_gen_fee_amount_usd,
        NULL AS mdf_amount_usd,
        NULL AS expected_collection_fee_amount_usd,
        NULL AS gmv_usd,
        NULL AS approved_amount_usd,
        NULL AS consumer_revenue_usd,
        NULL AS merchant_revenue_usd,
        NULL AS total_revenue_usd,
        NULL AS expected_final_losses,
        NULL AS is_first_slug_origination,
        NULL AS is_existing_shop_ally_before_current_year,
        NULL AS payment_cost_usd,
        NULL AS cost_to_serve_usd,
        NULL AS underwriting_cost_usd,
        NULL AS cost_of_funding_usd,
        NULL AS numerator_wal,
        NULL AS numerator_weighted_wal_usd,
        NULL AS numerator_weighted_term_usd,
        NULL AS synthetic_origination_marketplace_purchase_fee_amount_usd,
        SUM(dq31_at_31_upb) AS dq31_at_31_upb,
        SUM(dq31_at_31_opb) AS dq31_at_31_opb
    FROM  originations_baseline
    WHERE   dq31_at_31_date::DATE >= DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '19 month'
        AND dq31_at_31_date::DATE < CURRENT_DATE()
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
)
,
low_granularity_originations_and_losses_results AS (
    -- V1: Context - Alexis & Julio
    -- UNION of the aggregated origination dataset (based on the origination date) and the aggregated
    -- losses dataset (based on the losses date)
    (SELECT * FROM origination_date_metrics)
    UNION ALL
    (SELECT * FROM losses_date_metrics)
)
,
low_granularity_results AS (
    -- V1: Context - Alexis & Julio
    -- Aggregating both datasets based on the period_date in order to reduce complexity.
    -- Low granularity dataset to operate most dashboards, specially on amounts. Setting high-granularity columns as
    -- NULL so we can later join both dataset (low and high granularity)
    SELECT
        -- SHARE GRANULARITY DATASETS COLUMNS
        'AGGREGATED' AS dataset_granularity_type,
        o.country_code,
        o.period_date,
        o.term,
        o.client_type,
        o.application_channel,
        o.synthetic_channel,
        o.processed_product,
        o.synthetic_product_category,
        o.synthetic_product_subcategory,
        o.is_addishop_referral,
        o.is_addishop_referral_paid,
        o.ally_slug,
        o.store_slug,
        o.ally_vertical,
        o.ally_brand,
        o.ally_cluster,
        o.guarantee_provider,
        o.segment,
        o.is_santander,
        o.has_fga_flag,
        o.first_slug_origination_date,
        -- LOW-GRANULARITY DATASET COLUMNS
        SUM(o.total_interest) AS total_interest,
        SUM(o.guarantee_amount) AS guarantee_amount,
        SUM(o.gross_expected_final_losses_amount) AS gross_expected_final_losses_amount,
        SUM(o.guarantee_expected_loss_recovery_amount) AS guarantee_expected_loss_recovery_amount,
        SUM(o.fng_cost_amount) AS fng_cost_amount,
        SUM(o.lead_gen_fee_amount) AS lead_gen_fee_amount,
        SUM(o.mdf_amount) AS mdf_amount,
        SUM(o.expected_collection_fee_amount) AS expected_collection_fee_amount,
        SUM(o.gmv) AS gmv,
        SUM(o.approved_amount) AS approved_amount,
        SUM(o.approved_amount_filtered_for_losses) AS approved_amount_filtered_for_losses,
        SUM(o.synthetic_origination_marketplace_purchase_fee_amount) AS synthetic_origination_marketplace_purchase_fee_amount,
        SUM(o.consumer_revenue) AS consumer_revenue,
        SUM(o.merchant_revenue) AS merchant_revenue,
        SUM(o.total_revenue) AS total_revenue,
        SUM(o.total_interest_usd) AS total_interest_usd,
        SUM(o.guarantee_amount_usd) AS guarantee_amount_usd,
        SUM(o.gross_expected_final_losses_amount_usd) AS gross_expected_final_losses_amount_usd,
        SUM(o.guarantee_expected_loss_recovery_amount_usd) AS guarantee_expected_loss_recovery_amount_usd,
        SUM(o.fng_cost_amount_usd) AS fng_cost_amount_usd,
        SUM(o.lead_gen_fee_amount_usd) AS lead_gen_fee_amount_usd,
        SUM(o.mdf_amount_usd) AS mdf_amount_usd,
        SUM(o.expected_collection_fee_amount_usd) AS expected_collection_fee_amount_usd,
        SUM(o.gmv_usd) AS gmv_usd,
        SUM(o.approved_amount_usd) AS approved_amount_usd,
        SUM(o.consumer_revenue_usd) AS consumer_revenue_usd,
        SUM(o.merchant_revenue_usd) AS merchant_revenue_usd,
        SUM(o.total_revenue_usd) AS total_revenue_usd,
        SUM(CASE WHEN o.country_code = 'BR' THEN dq31_at_31_upb ELSE o.expected_final_losses END) AS expected_final_losses,
        MAX(o.is_first_slug_origination) AS is_first_slug_origination,
        MAX(o.is_existing_shop_ally_before_current_year) AS is_existing_shop_ally_before_current_year,
        SUM(o.dq31_at_31_upb) AS dq31_at_31_upb,
        SUM(o.dq31_at_31_opb) AS dq31_at_31_opb,
        MAX(fmm.cts_uw) AS cts_uw,
        MAX(fmm.total_cts_uw) AS total_cts_uw,
        MAX(fmm.cof) AS cof,
        MAX(fmm.total_cof) AS total_cof,
        SUM(o.payment_cost_usd) AS payment_cost_usd,
        SUM(o.cost_to_serve_usd) AS cost_to_serve_usd,
        SUM(o.underwriting_cost_usd) AS underwriting_cost_usd,
        SUM(o.cost_of_funding_usd) AS cost_of_funding_usd,
        SUM(o.numerator_wal) AS numerator_wal,
        SUM(o.numerator_weighted_wal_usd) AS numerator_weighted_wal_usd,
        SUM(o.numerator_weighted_term_usd) AS numerator_weighted_term_usd,
        SUM(o.synthetic_origination_marketplace_purchase_fee_amount_usd) AS synthetic_origination_marketplace_purchase_fee_amount_usd,
        -- HIGH-GRANULARITY DATASET COLUMNS -- Context link: PENDING
        ANY_VALUE(NULL) AS application_id,
        ANY_VALUE(NULL) AS custom_application_suborder_pairing_id,
        ANY_VALUE(NULL) AS loan_id
    FROM      low_granularity_originations_and_losses_results AS o
    LEFT JOIN funding_manual_metrics                          AS fmm ON  fmm.country_code = o.country_code
                                                                     AND fmm.period = DATE_TRUNC('month',o.period_date)
                                                                     AND fmm.synthetic_product_category = o.synthetic_product_category
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
)
,
high_granularity_rows AS (
    -- V2: Context - Carlos
    -- High granularity dataset to properly calculate counts (originations, suborders, loans) in Looker (lookml)
    -- having the option of applying dynamic filters based on shared granularity columns. Setting low-granularity
    -- columns as NULL so we can later join both dataset (low and high granularity)
    SELECT
        -- SHARE GRANULARITY DATASETS COLUMNS
        'DISAGGREGATED' AS dataset_granularity_type,
        country_code,
        origination_date_local AS period_date,
        term,
        client_type,
        application_channel,
        synthetic_channel,
        processed_product,
        synthetic_product_category,
        synthetic_product_subcategory,
        is_addishop_referral,
        is_addishop_referral_paid,
        ally_slug,
        store_slug,
        ally_vertical,
        ally_brand,
        ally_cluster,
        guarantee_provider,
        segment,
        is_santander,
        has_fga_flag,
        first_slug_origination_date,
        -- LOW-GRANULARITY DATASET COLUMNS
        NULL AS total_interest,
        NULL AS guarantee_amount,
        NULL AS gross_expected_final_losses_amount,
        NULL AS guarantee_expected_loss_recovery_amount,
        NULL AS fng_cost_amount,
        NULL AS lead_gen_fee_amount,
        NULL AS mdf_amount,
        NULL AS expected_collection_fee_amount,
        NULL AS gmv,
        NULL AS approved_amount,
        NULL AS approved_amount_filtered_for_losses,
        NULL AS synthetic_origination_marketplace_purchase_fee_amount,
        NULL AS consumer_revenue,
        NULL AS merchant_revenue,
        NULL AS total_revenue,
        NULL AS total_interest_usd,
        NULL AS guarantee_amount_usd,
        NULL AS gross_expected_final_losses_amount_usd,
        NULL AS guarantee_expected_loss_recovery_amount_usd,
        NULL AS fng_cost_amount_usd,
        NULL AS lead_gen_fee_amount_usd,
        NULL AS mdf_amount_usd,
        NULL AS expected_collection_fee_amount_usd,
        NULL AS gmv_usd,
        NULL AS approved_amount_usd,
        NULL AS consumer_revenue_usd,
        NULL AS merchant_revenue_usd,
        NULL AS total_revenue_usd,
        NULL AS expected_final_losses,
        is_first_slug_origination,
        is_existing_shop_ally_before_current_year,
        NULL AS dq31_at_31_upb,
        NULL AS dq31_at_31_opb,
        NULL AS cts_uw,
        NULL AS total_cts_uw,
        NULL AS cof,
        NULL AS total_cof,
        NULL AS payment_cost_usd,
        NULL AS cost_to_serve_usd,
        NULL AS underwriting_cost_usd,
        NULL AS cost_of_funding_usd,
        NULL AS numerator_wal,
        NULL AS numerator_weighted_wal_usd,
        NULL AS numerator_weighted_term_usd,
        NULL AS synthetic_origination_marketplace_purchase_fee_amount_usd,
        -- HIGH-GRANULARITY DATASET COLUMNS -- Context link: PENDING
        application_id,
        custom_application_suborder_pairing_id,
        loan_id
    FROM originations_baseline
    -- Same conditions as in `origination_date_metrics` CTE
    WHERE origination_date_local < CURRENT_DATE()
)
,
final_results AS (
    -- V2: Context - Carlos
    (SELECT * FROM low_granularity_results)
    UNION ALL
    (SELECT * FROM high_granularity_rows)
)
SELECT
    *,
    -- DATA PLATFORM COLUMNS
    NOW() AS ingested_at,
    TO_TIMESTAMP('{{ var("execution_date") }}') AS updated_at
FROM final_results