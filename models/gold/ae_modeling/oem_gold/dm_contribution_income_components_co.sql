{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

-- AE MODELIING: Transactional contribution income amount (loan_id based).

--      A few considerations for this release (against the previous one):

-- 1. Event though it's loan_id based (given the manual inputs dimensions), we preserve the suborder granularity to avoid the addi-marketplace slug values.
-- 2. We're replacing the legacy source for manual inputs (agg_addi_management_manual_metrics_inputs_txn) for the newest one: agg_manual_inputs_actuals_finance_co with a data contract related
--       Data contract here: https://www.notion.so/addico/FINANCE-actuals-Manual-inputs-87132b21ccb44ccda484fc2542b2d6cd?pvs=4
-- 3. Underwriting costs: Joining by loan_id only for PROSPECTS (instead of using the ratio prospects apps / total apps)
-- 4. Expected losses extrapolation for missing loans: Just in case there are loans without EL, then we use the daily average so we can compute the CI$ for every loan.
-- 5. We only provide amounts instead of ratios becasue of the bias created with a fixed grannularity.

WITH dm_originations_detailed_by_suborder AS (
    SELECT
        *,
        CASE
          WHEN dmo.synthetic_product_subcategory IN ('GRANDE', 'CUPO Flex', 'INTRO') THEN 'interest_bearing'
          ELSE 'interest_free'
        END AS custom_interest_category
    FROM {{ ref('dm_originations_detailed_by_suborder') }} dmo
)
,
agg_manual_inputs_actuals_finance_co AS (
    SELECT
        mi.country_code,
        mi.period,
        mi.period_type,
        mi.metric_name,
        mi.product,
        mi.interest_category,
        CAST(mi.term AS DOUBLE) AS term,
        CAST(mi.value AS DOUBLE) AS value
    FROM {{ source('gold', 'agg_manual_inputs_actuals_finance_co') }} mi
)
,
originations_baseline AS (
    SELECT
        dmo.*,
        COALESCE(dmo.debug_attribution_weight, 1) AS custom_debug_attribution_weight,
        COALESCE(pc.value, 0) AS payment_cost_raw_amount,
        COALESCE(cts.value, 0) AS cost_to_serve_raw_amount,
        COALESCE(uw.value, 0) AS underwriting_cost_raw_amount,
        COALESCE(cof_grande.value, 0) AS cost_of_funding_grande_raw_amount,
        COALESCE(cof_cupo.value, 0) AS cost_of_funding_cupo_raw_amount,
        COALESCE(wal.value, 0) AS wal_raw_amount
    FROM dm_originations_detailed_by_suborder dmo
    LEFT JOIN agg_manual_inputs_actuals_finance_co AS pc         ON dmo.country_code = pc.country_code
                                                                AND DATE_TRUNC('month', dmo.origination_date_local)::DATE = pc.period
                                                                AND pc.metric_name = 'payment_cost'
    LEFT JOIN agg_manual_inputs_actuals_finance_co AS cts        ON dmo.country_code = cts.country_code
                                                                AND DATE_TRUNC('month', dmo.origination_date_local)::DATE = cts.period
                                                                AND cts.metric_name = 'cost_to_serve'
    LEFT JOIN agg_manual_inputs_actuals_finance_co AS uw         ON dmo.country_code = uw.country_code
                                                                AND DATE_TRUNC('month', dmo.origination_date_local)::DATE = uw.period
                                                                AND dmo.synthetic_product_category = uw.product
                                                                AND uw.metric_name = 'underwriting_cost'
                                                                AND dmo.client_type = 'PROSPECT'
    LEFT JOIN agg_manual_inputs_actuals_finance_co AS cof_grande ON dmo.country_code = cof_grande.country_code
                                                                AND DATE_TRUNC('month', dmo.origination_date_local)::DATE = cof_grande.period
                                                                AND ROUND(dmo.term * 0.95, 1) = ROUND(cof_grande.term, 1)
                                                                AND dmo.synthetic_product_category = cof_grande.product
                                                                AND cof_grande.metric_name = 'cost_of_funding'
                                                                AND cof_grande.product= 'GRANDE'
    LEFT JOIN agg_manual_inputs_actuals_finance_co AS cof_cupo   ON dmo.country_code = cof_cupo.country_code
                                                                AND DATE_TRUNC('month', dmo.origination_date_local)::DATE = cof_cupo.period
                                                                AND dmo.term  = cof_cupo.term
                                                                AND dmo.synthetic_product_category = cof_cupo.product
                                                                AND cof_cupo.metric_name = 'cost_of_funding'
                                                                AND cof_cupo.product IN ('INTRO', 'CUPO')
    LEFT JOIN agg_manual_inputs_actuals_finance_co AS wal        ON dmo.country_code = wal.country_code
                                                                AND DATE_TRUNC('year', dmo.origination_date_local)::DATE = wal.period
                                                                AND dmo.term = wal.term
                                                                AND dmo.custom_interest_category = wal.interest_category
                                                                AND wal.metric_name = 'wal'
)
,
manual_inputs_by_suborder_step_1 AS (
    SELECT
        ob.*,
        COALESCE(ob.gmv, 0) / ob.fx_rate AS gmv_usd,
        COALESCE(ob.approved_amount, 0) / ob.fx_rate AS approved_amount_usd,
        COALESCE(ob.mdf_amount, 0) / ob.fx_rate AS mdf_amount_usd,
        COALESCE(ob.lead_gen_fee_amount, 0) / ob.fx_rate AS lead_gen_fee_amount_usd,
        COALESCE(ob.total_interest, 0) / ob.fx_rate AS total_interest_usd,
        COALESCE(ob.expected_collection_fee_amount, 0) / ob.fx_rate AS expected_collection_fee_usd,
        COALESCE(ob.guarantee_amount, 0) / ob.fx_rate AS guarantee_amount_usd,
        COALESCE(ob.expected_final_losses, 0) / ob.fx_rate AS expected_final_losses_usd,
        AVG(COALESCE(ob.expected_final_losses, 0) / ob.fx_rate) OVER( PARTITION BY to_date(ob.origination_date_local)) AS expected_final_losses_usd_extrapolation,
        COALESCE(ob.approved_amount_filtered_for_losses, 0) / ob.fx_rate AS approved_amount_filtered_for_losses_usd,
        COALESCE(ob.synthetic_origination_marketplace_purchase_fee_amount, 0) / ob.fx_rate AS synthetic_origination_marketplace_purchase_fee_amount_usd,
        COALESCE(ob.payment_cost_raw_amount * ob.term, 0) * ob.custom_debug_attribution_weight / ob.fx_rate AS payment_cost_usd,
        COALESCE(ob.cost_to_serve_raw_amount * ob.term, 0) * ob.custom_debug_attribution_weight / ob.fx_rate AS cost_to_serve_usd,
        COALESCE(ob.underwriting_cost_raw_amount, 0) * ob.custom_debug_attribution_weight / ob.fx_rate AS underwriting_cost_usd,
        COALESCE(ob.cost_of_funding_grande_raw_amount, 0) * ob.gmv / ob.fx_rate AS cost_of_funding_grande_usd,
        COALESCE(ob.cost_of_funding_cupo_raw_amount, 0) * ob.gmv / ob.fx_rate AS cost_of_funding_cupo_usd,
        COALESCE(ob.fng_cost_amount, 0) / ob.fx_rate AS fng_cost_amount_usd,
        COALESCE(ob.wal_raw_amount, 0) AS wal
    FROM originations_baseline ob
)
,
manual_inputs_by_suborder_step_2 AS (
    SELECT
        *,
        -- Just in case the expected losses are missing (almsost never), then We infer the value by using the daily average.
        COALESCE(mi.expected_final_losses, mi.expected_final_losses_usd_extrapolation) AS synthetic_expected_final_losses
    FROM manual_inputs_by_suborder_step_1 mi
)
,
manual_inputs_by_suborder_step_3 AS (
    SELECT
        mi.country_code,
        mi.application_id,
        mi.custom_application_suborder_pairing_id,
        mi.loan_id,
        mi.client_id,
        mi.origination_date_local,
        mi.ally_slug,
        mi.custom_debug_attribution_weight,
        mi.term,
        mi.client_type,
        mi.synthetic_product_category,
        mi.synthetic_product_subcategory,
        mi.gmv_usd,
        mi.mdf_amount_usd + mi.lead_gen_fee_amount_usd + mi.synthetic_origination_marketplace_purchase_fee_amount_usd +
          mi.total_interest_usd + mi.expected_collection_fee_usd AS total_revenue_usd,
        mi.guarantee_amount_usd,
        mi.fng_cost_amount_usd,
        mi.synthetic_expected_final_losses AS expected_final_losses_usd,
        mi.approved_amount_filtered_for_losses_usd,
        mi.payment_cost_usd,
        mi.cost_to_serve_usd,
        mi.underwriting_cost_usd,
        CASE
          WHEN mi.synthetic_product_category IN ('CUPO', 'INTRO') THEN mi.cost_of_funding_cupo_usd
          WHEN mi.synthetic_product_category = 'GRANDE' THEN mi.cost_of_funding_grande_usd * (1 - (mi.synthetic_expected_final_losses / mi.approved_amount_filtered_for_losses_usd) * 0.5)
          ELSE 0
        END AS cost_of_funding_usd
    FROM manual_inputs_by_suborder_step_2 mi
)
SELECT
    *,
    total_revenue_usd +
        guarantee_amount_usd -
        expected_final_losses_usd -
        payment_cost_usd -
        cost_to_serve_usd -
        underwriting_cost_usd -
        cost_of_funding_usd -
        fng_cost_amount_usd AS contribution_income_usd
FROM manual_inputs_by_suborder_step_3