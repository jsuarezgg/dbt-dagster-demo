{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
{%- set attribution_weight_column = 'suborder_attribution_weight_by_total_without_discount_amount' %}
WITH
dm_originations AS (
    SELECT *
    FROM {{ ref('dm_originations') }}
)
,
dm_originations_marketplace_suborders_co AS (
    SELECT *
    FROM {{ ref('dm_originations_marketplace_suborders_co') }}
)
SELECT
    -- PK AND ROW TYPE
    MD5(COALESCE(mktplc_o.custom_application_suborder_pairing_id, o.application_id)) AS row_id,
    -- A. KEY FIELDS
    o.country_code,
    o.application_id,
    mktplc_o.custom_application_suborder_pairing_id,
    o.loan_id,
    o.client_id,
    o.refinanced_by_origination_of_loan_id,
    o.order_id,
    COALESCE(mktplc_o.suborder_ally_slug, o.ally_slug) AS ally_slug,
    -- B. ORIGINATIONS DATA
    COALESCE(mktplc_o.suborder_store_slug, o.store_slug) AS store_slug,
    o.origination_date,
    o.origination_date_local,
    o.origination_hour_local,
    o.origination_minute_local,
    o.lbl,
    o.term,
    o.credit_policy_name,
    o.pd_calculation_method,
    -- C. APPLICATIONS FIELDS
    o.client_type,
    o.journey_name,
    o.application_channel,
    o.synthetic_channel,
    o.original_product,
    o.processed_product,
    o.synthetic_product_category,
    o.synthetic_product_subcategory,
    o.santander_branched,
    o.santander_origination,
    -- D. AMOUNTS AND RATES - REVENUE METRICS
    COALESCE(mktplc_o.{{ attribution_weight_column }} * o.approved_amount, o.approved_amount) AS approved_amount,
    COALESCE(mktplc_o.synthetic_suborder_total_amount, o.requested_amount) AS requested_amount,
    COALESCE(mktplc_o.synthetic_suborder_total_amount, o.gmv) AS gmv,
    o.guarantee_rate,
    o.guarantee_provider_with_default,
    COALESCE(mktplc_o.{{ attribution_weight_column }} * o.fng_cost_amount, o.fng_cost_amount) AS fng_cost_amount,
    COALESCE(mktplc_o.{{ attribution_weight_column }} * o.guarantee_amount_charged_at_checkout, o.guarantee_amount_charged_at_checkout) AS guarantee_amount_charged_at_checkout,
    COALESCE(mktplc_o.{{ attribution_weight_column }} * o.guarantee_amount, o.guarantee_amount) AS guarantee_amount,
    o.has_fga_flag,
    o.interest_rate,
    COALESCE(mktplc_o.{{ attribution_weight_column }} * o.total_interest, o.total_interest) AS total_interest,
    o.lead_gen_fee_rate,
    COALESCE(mktplc_o.{{ attribution_weight_column }} * o.lead_gen_fee_amount, o.lead_gen_fee_amount) AS lead_gen_fee_amount,
    COALESCE(mktplc_o.suborder_origination_mdf, o.ally_mdf) AS ally_mdf,
    COALESCE(mktplc_o.synthetic_suborder_origination_mdf_amount, o.mdf_amount) AS mdf_amount,
    COALESCE(mktplc_o.suborder_marketplace_purchase_fee, o.synthetic_origination_marketplace_purchase_fee) AS synthetic_origination_marketplace_purchase_fee,
    COALESCE(mktplc_o.synthetic_suborder_marketplace_purchase_fee_amount, o.synthetic_origination_marketplace_purchase_fee_amount) AS synthetic_origination_marketplace_purchase_fee_amount,
    COALESCE(mktplc_o.{{ attribution_weight_column }} * o.expected_collection_fee_amount, o.expected_collection_fee_amount) AS expected_collection_fee_amount,
    COALESCE(mktplc_o.{{ attribution_weight_column }} * o.consumer_revenue, o.consumer_revenue) AS consumer_revenue,
    COALESCE(mktplc_o.{{ attribution_weight_column }} * o.merchant_revenue, o.merchant_revenue) AS merchant_revenue,
    COALESCE(mktplc_o.{{ attribution_weight_column }} * o.total_revenue, o.total_revenue) AS total_revenue,
    -- E. ALLY DATA
    COALESCE(mktplc_o.suborder_ally_vertical, o.ally_vertical) AS ally_vertical,
    COALESCE(mktplc_o.suborder_ally_brand, o.ally_brand) AS ally_brand,
    COALESCE(mktplc_o.suborder_ally_cluster, o.ally_cluster) AS ally_cluster,
    COALESCE(mktplc_o.account_kam_name, o.account_kam_name) AS account_kam_name,
    -- F. LOAN PERFORMANCE DATA + LOSSES FIELDS
    COALESCE(mktplc_o.{{ attribution_weight_column }} * o.approved_amount_filtered_for_losses, o.approved_amount_filtered_for_losses) AS approved_amount_filtered_for_losses,
    COALESCE(mktplc_o.{{ attribution_weight_column }} * o.guarantee_expected_loss_recovery_amount, o.guarantee_expected_loss_recovery_amount) AS guarantee_expected_loss_recovery_amount,
    COALESCE(mktplc_o.{{ attribution_weight_column }} * o.gross_expected_final_losses_amount, o.gross_expected_final_losses_amount) AS gross_expected_final_losses_amount,
    COALESCE(mktplc_o.{{ attribution_weight_column }} * o.expected_final_losses, o.expected_final_losses) AS expected_final_losses,
    o.expected_final_losses_rate,
    COALESCE(mktplc_o.{{ attribution_weight_column }} * o.dq31_at_31_upb,o.dq31_at_31_upb) AS dq31_at_31_upb,
    COALESCE(mktplc_o.{{ attribution_weight_column }} * o.dq31_at_31_opb,o.dq31_at_31_opb) AS dq31_at_31_opb,
    o.dq31_at_31_date,
    -- G. ADDI SHOP REFERRAL DATA
    o.is_addishop_referral,
    o.is_addishop_referral_paid,
    o.addishop_channel,
    o.shop_used_grouped_config,
    o.addishop_opt_in_date,
    o.addishop_opt_out_date,
    -- I. OTHER FIELDS
    o.fx_rate,
    o.segment, 
    CASE WHEN mktplc_o.custom_application_suborder_pairing_id IS NOT NULL THEN 'MARKETPLACE_SUBORDER_ORIGINATION' ELSE 'NON_MARKETPLACE_ORIGINATION' END AS debug_row_granularity,
    mktplc_o.{{ attribution_weight_column }} AS debug_attribution_weight,
    ARRAY_SIZE(o.suborders_ally_slug_array) AS debug_in_marketplace_order_num_allies,
    (o.application_channel = 'ADDI_MARKETPLACE' AND mktplc_o.application_id IS NULL AND o.origination_date <= (NOW()- INTERVAL '4' HOUR)) AS debug_suborders_quality_alert_trigger,
    -- J. DATA PLATFORM DATA
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM      dm_originations                          AS o
LEFT JOIN dm_originations_marketplace_suborders_co AS mktplc_o ON o.application_id = mktplc_o.application_id -- 1:N relationship