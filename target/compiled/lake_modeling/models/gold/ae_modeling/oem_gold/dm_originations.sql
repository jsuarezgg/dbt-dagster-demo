
--        materialized='incremental',
--        unique_key='application_id',
--        incremental_strategy='merge',
-- AE - Alexis de la Fuente: Incremental approach as the one we use with the silver builder
WITH
f_originations_bnpl_co AS (
    SELECT *
    FROM silver.f_originations_bnpl_co)
,
f_originations_bnpn_co AS (
    SELECT *
    FROM silver.f_originations_bnpn_co)
,
f_originations_bnpl_br AS (
    SELECT *
    FROM silver.f_originations_bnpl_br)
,
f_originations_bnpn_br AS (
    SELECT *
    FROM silver.f_originations_bnpn_br)
,
bl_application_channel_co AS (
    SELECT *
    FROM gold.bl_application_channel
    WHERE country_code = 'CO')
,
bl_application_channel_br AS (
    SELECT *
    FROM gold.bl_application_channel
    WHERE country_code = 'BR')
,
risk_master_table_co AS (
    SELECT DISTINCT 
           loan_id,
           dpd_plus_1_month,
           UPB_plus_1_month,
           approved_amount,
           FP_date_plus_1_month
    FROM gold.risk_master_table_co)
,
risk_master_table_br AS (
    SELECT DISTINCT 
           loan_id,
           dpd_plus_1_month,
           UPB_plus_1_month,
           general_amount,
           FP_date_plus_1_month
    FROM gold.risk_master_table_br)
,
f_applications_co AS (
    SELECT *
    FROM silver.f_applications_co)
,
f_applications_br AS (
    SELECT *
    FROM silver.f_applications_br)
,
f_loan_proposals_co AS (
    SELECT *
    FROM silver.f_loan_proposals_co)
,
f_loan_proposals_br AS (
    SELECT *
    FROM silver.f_loan_proposals_br)
,
f_underwriting_fraud_stage_co AS (
    SELECT *
    FROM silver.f_underwriting_fraud_stage_co)
,
f_underwriting_fraud_stage_br AS (
    SELECT *
    FROM silver.f_underwriting_fraud_stage_br)
,
f_allies_product_policies_co AS (
    SELECT *
    FROM silver.f_allies_product_policies_co)
,
bl_origination_addi_shop_co AS (
    SELECT *
    FROM gold.bl_origination_addi_shop
    WHERE country_code = 'CO')
,
bl_origination_addi_shop_br AS (
    SELECT *
    FROM gold.bl_origination_addi_shop
    WHERE country_code = 'BR')
,
bl_application_product_co AS (
    SELECT *
    FROM gold.bl_application_product_co)
,
bl_application_product_br AS (
    SELECT *
    FROM gold.bl_application_product_br)
,
bl_ally_brand_ally_slug_status_co AS (
    SELECT *
    FROM gold.bl_ally_brand_ally_slug_status
    WHERE country_code = 'CO'
)
,
bl_ally_brand_ally_slug_status_br AS (
    SELECT *
    FROM gold.bl_ally_brand_ally_slug_status
    WHERE country_code = 'BR'
)
,
bl_originations_payment_date_co AS (
    SELECT *
    FROM gold.bl_originations_payment_date_co
)
,
--https://addico.slack.com/archives/C05F4587X27/p1688162477828489
grande_originations_forced_losses_prob AS (
	 SELECT
        o.loan_id,
        o.origination_date,
        o.approved_amount*0.07 AS forced_pred_co_bal_legacy, -- 0.07 fixed requested by business
        ap.synthetic_product_category
     FROM       f_originations_bnpl_co AS o
     INNER JOIN bl_application_product_co AS ap ON o.application_id = ap.application_id
     WHERE o.origination_date::date>='2023-06-01' and ap.synthetic_product_category='GRANDE'
)
,
expected_losses_table AS (
    SELECT DISTINCT
        loan_id,
        co_factor,
        prediction_31_31_unit,
        null AS pred_co_bal,
        0 AS is_grande_risk_product_category
    FROM risk.prospect_loss_forecast 
    UNION ALL
    SELECT DISTINCT 
        loan_id,
        co_factor,
        prediction_31_31_unit_adjusted AS prediction_31_31_unit,
        null AS pred_co_bal,
        0 AS is_grande_risk_product_category
    FROM risk.client_loss_forecast
    UNION ALL
    --Compra Grande Expected Losses
    SELECT DISTINCT
        COALESCE(a.loan_id,b.loan_id) AS loan_id,
        null AS co_factor,
        null AS prediction_31_31_unit,
        FIRST(COALESCE(a.net_expected_lifetime_losses,b.forced_pred_co_bal_legacy),TRUE) AS pred_co_bal,
        1 AS is_grande_risk_product_category
    FROM silver.surv_model_results AS a
    FULL OUTER JOIN grande_originations_forced_losses_prob AS b ON a.loan_id = b.loan_id
    GROUP BY 1,2,3,5
)
,
expected_collection_fees AS (
    SELECT DISTINCT 
        loan_id,
        collection_fee_income_amount
    FROM silver.ds_expected_collection_fees_results
) 
,
br_bad_book_clients AS (
    SELECT
        client_id,
        'seg1' AS segment
    FROM risk.seg1_cut_br_v2 
    UNION ALL
    SELECT
        client_id,
        'seg2' AS segment
    FROM risk.seg2_cut_br_v2 
)
,
joined_table AS (
SELECT "CO" AS country_code
      ,ob.loan_id
      ,app.application_id
      ,app.order_id
      ,ob.client_id
      ,ob.origination_date
      ,from_utc_timestamp(ob.origination_date,"America/Bogota") AS origination_date_local
      ,hour(from_utc_timestamp(ob.origination_date,"America/Bogota")) AS origination_hour_local
      ,minute(from_utc_timestamp(ob.origination_date,"America/Bogota")) AS origination_minute_local
      ,ob.approved_amount -- lo dejamos?
      ,CASE WHEN app.requested_amount_without_discount >0 THEN app.requested_amount_without_discount
            ELSE app.requested_amount
       END AS requested_amount
      ,l.ally_mdf
      ,ob.guarantee_rate
      ,l.interest_rate
      ,l.total_interest
      ,m.is_addishop_referral
      ,m.addishop_channel
      ,m.is_addishop_referral_paid
      ,m.used_grouped_config AS shop_used_grouped_config
      ,m.addi_shop_ally_period_opt_in_date AS addishop_opt_in_date
      ,m.addi_shop_ally_period_opt_out_date AS addishop_opt_out_date
      ,m.lead_gen_fee_rate
      ,COALESCE(ecf.collection_fee_income_amount, 0) AS collection_fee_income_amount 
      ,ob.lbl
      ,ob.term
      ,app.client_type
      ,null as segment
      ,app.journey_name
      ,ac.synthetic_channel
      ,u.credit_policy_name
      ,blap.original_product
      ,blap.processed_product
      ,blap.synthetic_product_category
      ,blap.synthetic_product_subcategory
      ,app.custom_is_santander_branched AS santander_branched
      ,ob.custom_is_santander_originated AS santander_origination  
      ,u.pd_calculation_method     
      ,ob.ally_slug
      ,ob.store_slug
      ,asl.ally_vertical
      ,asl.ally_brand 
      ,COALESCE(asl.ally_cluster,'KA') AS ally_cluster
      ,case when rmt.dpd_plus_1_month>1 then rmt.UPB_plus_1_month when rmt.dpd_plus_1_month<=1 then 0 else null end as dq31_at_31_upb
      ,case when rmt.dpd_plus_1_month is not null then rmt.approved_amount end as dq31_at_31_opb
      ,rmt.FP_date_plus_1_month as dq31_at_31_date
      ,CASE WHEN el.is_grande_risk_product_category = 0 THEN (el.prediction_31_31_unit * ob.approved_amount) * el.co_factor
            WHEN el.is_grande_risk_product_category  = 1 THEN el.pred_co_bal
      END AS expected_final_losses
      ,opd.payment_date
      ,opd.report_term
      ,opd.payment_term
      ,CASE WHEN opd.report_term = 'WEEKLY' THEN 4
            WHEN opd.report_term = 'MONTHLY' THEN 15
            ELSE datediff(opd.payment_date,ob.origination_date) 
      END AS diff_payment_date
FROM f_originations_bnpl_co ob
LEFT JOIN bl_application_channel_co ac ON ac.application_id=ob.application_id 
LEFT JOIN risk_master_table_co rmt ON rmt.loan_id=ob.loan_id
LEFT JOIN f_applications_co app ON app.application_id=ob.application_id
LEFT JOIN f_loan_proposals_co l ON ob.loan_id = l.loan_proposal_id
LEFT JOIN f_underwriting_fraud_stage_co u ON ob.application_id=u.application_id
LEFT JOIN bl_origination_addi_shop_co m ON ob.application_id = m.application_id 
LEFT JOIN bl_application_product_co blap ON blap.application_id = ob.application_id
LEFT JOIN expected_losses_table el ON ob.loan_id = el.loan_id
LEFT JOIN bl_originations_payment_date_co opd ON ob.loan_id = opd.loan_id
LEFT JOIN bl_ally_brand_ally_slug_status_co asl ON asl.ally_slug = ob.ally_slug
LEFT JOIN expected_collection_fees ecf ON ecf.loan_id = ob.loan_id
WHERE asl.ally_slug <> 'addi-preapprovals' --Son 9 originaciones de 2021 que vienen con este slug y preapproval as channel, las excluyo

UNION ALL

SELECT "CO" AS country_code
      ,null AS loan_id
      ,app.application_id
      ,app.order_id
      ,ob.client_id
      ,CAST(ob.last_event_ocurred_on_processed AS DATE) AS origination_date
      ,from_utc_timestamp(ob.last_event_ocurred_on_processed,"America/Bogota") AS origination_date_local
      ,hour(from_utc_timestamp(ob.last_event_ocurred_on_processed,"America/Bogota")) AS origination_hour_local
      ,minute(from_utc_timestamp(ob.last_event_ocurred_on_processed,"America/Bogota")) AS origination_minute_local
      ,ob.payment_amount AS approved_amount
      ,CASE WHEN app.requested_amount_without_discount >0 THEN app.requested_amount_without_discount
            ELSE app.requested_amount
       END AS requested_amount
      ,apol.origination_mdf as ally_mdf
      ,null AS guarantee_rate
      ,null AS interest_rate
      ,null AS total_interest
      ,m.is_addishop_referral
      ,m.addishop_channel
      ,m.is_addishop_referral_paid
      ,m.used_grouped_config AS shop_used_grouped_config
      ,m.addi_shop_ally_period_opt_in_date AS addishop_opt_in_date
      ,m.addi_shop_ally_period_opt_out_date AS addishop_opt_out_date
      ,m.lead_gen_fee_rate
      ,null AS collection_fee_income_amount
      ,null AS lbl
      ,null AS term
      ,app.client_type
      ,null AS segment
      ,null AS journey_name
      ,ac.synthetic_channel
      ,null AS credit_policy_name
      ,blap.original_product
      ,blap.processed_product
      ,blap.synthetic_product_category
      ,blap.synthetic_product_subcategory
      ,app.custom_is_santander_branched AS santander_branched
      ,FALSE AS santander_origination  
      ,null AS pd_calculation_method     
      ,ob.ally_slug
      ,ob.store_slug
      ,asl.ally_vertical
      ,asl.ally_brand 
      ,COALESCE(asl.ally_cluster,'KA') AS ally_cluster
      ,null AS dq31_at_31_upb
      ,null AS dq31_at_31_opb
      ,null AS dq31_at_31_date
      ,null AS expected_final_losses
      ,null AS payment_date
      ,null AS report_term
      ,null AS payment_term
      ,null AS diff_payment_date
FROM f_originations_bnpn_co ob
LEFT JOIN bl_application_channel_co ac ON ac.application_id=ob.application_id 
--LEFT JOIN risk_master_table_co rmt ON rmt.loan_id=ob.loan_id
LEFT JOIN f_applications_co app ON app.application_id=ob.application_id
--LEFT JOIN f_loan_proposals_co l ON ob.loan_id = l.loan_proposal_id
--LEFT JOIN f_underwriting_fraud_stage_co u ON ob.application_id=u.application_id
LEFT JOIN bl_origination_addi_shop_co m ON ob.application_id = m.application_id 
LEFT JOIN bl_application_product_co blap ON blap.application_id = ob.application_id
--LEFT JOIN expected_losses_table el ON ob.loan_id = el.loan_id
--LEFT JOIN bl_originations_payment_date_co opd ON ob.loan_id = opd.loan_id
LEFT JOIN bl_ally_brand_ally_slug_status_co asl ON asl.ally_slug = ob.ally_slug
LEFT JOIN f_allies_product_policies_co apol ON ob.application_id = apol.application_id
    AND apol.type = 'ADDI_BNPN' and apol.product = 'BNPN_CO'

UNION ALL

SELECT "BR" AS country_code
      ,ob.loan_id
      ,app.application_id
      ,app.order_id
      ,ob.client_id
      ,ob.origination_date
      ,from_utc_timestamp(ob.origination_date,"America/Sao_Paulo") AS origination_date_local
      ,hour(from_utc_timestamp(ob.origination_date,"America/Sao_Paulo")) AS origination_hour_local
      ,minute(from_utc_timestamp(ob.origination_date,"America/Sao_Paulo")) AS origination_minute_local
      ,ob.approved_amount
      ,CASE WHEN app.requested_amount_without_discount >0 THEN app.requested_amount_without_discount
            ELSE app.requested_amount
       END AS requested_amount
      ,l.ally_mdf
      ,null AS guarantee_rate
      ,l.interest_rate
      ,l.total_interest
      ,m.is_addishop_referral
      ,m.addishop_channel
      ,m.is_addishop_referral_paid
      , NULL AS shop_used_grouped_config
      ,m.addi_shop_ally_period_opt_in_date AS addishop_opt_in_date
      ,m.addi_shop_ally_period_opt_out_date AS addishop_opt_out_date
      ,m.lead_gen_fee_rate
      ,null AS collection_fee_income_amount
      ,ob.lbl
      ,ob.term
      ,app.client_type
      ,CASE WHEN s.client_id IS null THEN 'good_book'
            WHEN s.client_id IS NOT null THEN 'bad_book'
       END AS segment
      ,app.journey_name
      ,ac.synthetic_channel
      ,u.credit_policy_name   
      ,blap.original_product
      ,blap.processed_product
      ,blap.synthetic_product_category
      ,blap.synthetic_product_subcategory
      ,null AS santander_branched
      ,null AS santander_origination 
      ,u.pd_calculation_method
      ,ob.ally_slug
      ,ob.store_slug
      ,asl.ally_vertical
      ,asl.ally_brand
      ,COALESCE(asl.ally_cluster,'KA') AS ally_cluster
      ,case when rmt.dpd_plus_1_month>1 then rmt.UPB_plus_1_month when rmt.dpd_plus_1_month<=1 then 0 else null end as dq31_at_31_upb
      ,case when rmt.dpd_plus_1_month is not null then rmt.general_amount end as dq31_at_31_opb
      ,rmt.FP_date_plus_1_month dq31_at_31_date
      --, NULL AS prediction_31_31_unit
      , null AS expected_final_losses
      , null AS payment_date
      , null AS report_term
      , null AS payment_term
      , null AS diff_payment_date
FROM f_originations_bnpl_br ob
LEFT JOIN bl_application_channel_br ac ON ac.application_id=ob.application_id 
LEFT JOIN risk_master_table_br rmt ON rmt.loan_id=ob.loan_id
LEFT JOIN f_applications_br app ON app.application_id=ob.application_id
LEFT JOIN f_loan_proposals_br l ON ob.loan_id = l.loan_proposal_id
LEFT JOIN f_underwriting_fraud_stage_br u ON ob.application_id=u.application_id
LEFT JOIN bl_origination_addi_shop_br m ON ob.application_id = m.application_id 
LEFT JOIN bl_application_product_br blap ON blap.application_id = ob.application_id
LEFT JOIN expected_losses_table el ON ob.loan_id = el.loan_id
LEFT JOIN br_bad_book_clients s	ON s.client_id = ob.client_id
LEFT JOIN bl_ally_brand_ally_slug_status_br asl ON asl.ally_slug = ob.ally_slug

UNION ALL

SELECT "BR" AS country_code
      ,ob.application_id AS loan_id
      ,app.application_id
      , app.order_id
      ,ob.client_id
      ,ob.origination_date
      ,from_utc_timestamp(ob.origination_date,"America/Sao_Paulo") AS origination_date_local
      ,hour(from_utc_timestamp(ob.origination_date,"America/Sao_Paulo")) AS origination_hour_local
      ,minute(from_utc_timestamp(ob.origination_date,"America/Sao_Paulo")) AS origination_minute_local
      ,ob.requested_amount AS approved_amount
      ,CASE WHEN app.requested_amount_without_discount >0 THEN app.requested_amount_without_discount
            ELSE app.requested_amount
       END AS requested_amount
      ,l.ally_mdf
      ,null AS guarantee_rate
      ,l.interest_rate
      ,l.total_interest
      ,m.is_addishop_referral
      ,m.addishop_channel
      ,m.is_addishop_referral_paid
      , NULL AS shop_used_grouped_config
      ,m.addi_shop_ally_period_opt_in_date AS addishop_opt_in_date
      ,m.addi_shop_ally_period_opt_out_date AS addishop_opt_out_date
      ,m.lead_gen_fee_rate
      ,null AS collection_fee_income_amount
      ,null AS lbl
      ,null AS term
      ,app.client_type
      ,CASE WHEN s.client_id IS null THEN 'good_book'
        WHEN s.client_id IS NOT null THEN 'bad_book'
        END AS segment
      ,app.journey_name
      ,ac.synthetic_channel
      ,u.credit_policy_name
      ,blap.original_product
      ,blap.processed_product
      ,blap.synthetic_product_category
      ,blap.synthetic_product_subcategory
      ,null AS santander_branched
      ,null AS santander_origination
      ,u.pd_calculation_method
      ,ob.ally_slug
      ,app.store_slug
      ,asl.ally_vertical
      ,asl.ally_brand
      ,COALESCE(asl.ally_cluster,'KA') AS ally_cluster
      ,null AS dq31_at_31_upb
      ,null AS dq31_at_31_opb
      ,null AS dq31_at_31_date
      ,null AS expected_final_losses
      ,null AS payment_date
      ,null AS report_term
      ,null AS payment_term
      ,null AS diff_payment_date
FROM f_originations_bnpn_br ob
LEFT JOIN bl_application_channel_br ac ON ac.application_id=ob.application_id
LEFT JOIN f_applications_br app ON app.application_id=ob.application_id
LEFT JOIN f_loan_proposals_br l ON ob.application_id = l.loan_proposal_id
LEFT JOIN f_underwriting_fraud_stage_br u ON ob.application_id=u.application_id
LEFT JOIN bl_origination_addi_shop_br m ON ob.application_id = m.application_id 
LEFT JOIN bl_application_product_br blap ON blap.application_id = ob.application_id
LEFT JOIN br_bad_book_clients s	ON s.client_id = ob.client_id
LEFT JOIN bl_ally_brand_ally_slug_status_br asl ON asl.ally_slug = ob.ally_slug
)
, transformed_table AS (
SELECT jt.country_code
      ,loan_id
      ,application_id
      ,order_id
      ,client_id
      ,origination_date
      ,origination_date_local
      ,origination_hour_local
      ,origination_minute_local
      ,approved_amount
      ,CASE WHEN expected_final_losses IS NOT NULL THEN approved_amount
            ELSE 0 
       END AS approved_amount_filtered_for_losses
      ,requested_amount
      ,requested_amount AS gmv
      ,ally_mdf
      ,CASE WHEN santander_origination IS NOT true THEN requested_amount
       ELSE 0
       END AS requested_amount_co
      ,guarantee_rate
      ,interest_rate
      ,CASE WHEN santander_origination IS NOT true THEN total_interest
            ELSE 0
       END AS total_interest
      ,is_addishop_referral
      , addishop_channel
      ,is_addishop_referral_paid
      ,shop_used_grouped_config
      , addishop_opt_in_date
      , addishop_opt_out_date
      ,lead_gen_fee_rate
      ,collection_fee_income_amount
      ,fr.price AS fx_rate
      ,lbl
      ,term
      ,client_type
      ,segment
      ,journey_name
      ,synthetic_channel
      ,credit_policy_name   
      ,original_product
      ,processed_product
      ,synthetic_product_category
      ,synthetic_product_subcategory
      ,santander_branched
      ,santander_origination  
      ,pd_calculation_method     
      ,ally_slug
      ,store_slug
      ,ally_vertical
      ,ally_brand 
      ,ally_cluster
      ,dq31_at_31_upb
      ,dq31_at_31_opb
      ,dq31_at_31_date 
      ,expected_final_losses
      ,payment_date
      , report_term
      ,payment_term
      ,diff_payment_date
FROM joined_table jt
LEFT JOIN silver.d_fx_rate fr ON fr.country_code = jt.country_code AND fr.is_active IS true
)
, amounts_table AS (
SELECT country_code
      ,loan_id
      ,application_id
      ,order_id
      ,client_id
      ,origination_date
      ,origination_date_local
      ,origination_hour_local
      ,origination_minute_local
      ,approved_amount
      ,approved_amount_filtered_for_losses
      ,requested_amount
      ,gmv
      ,ally_mdf
      ,CASE WHEN country_code = 'CO' THEN gmv * COALESCE(ally_mdf,0) 
            ELSE gmv * COALESCE(ally_mdf,0)
       END AS mdf_amount
      ,guarantee_rate
      ,CASE WHEN country_code = 'CO' THEN requested_amount_co * COALESCE(guarantee_rate,0) 
            ELSE gmv * COALESCE(guarantee_rate,0)
       END AS guarantee_amount
      ,interest_rate
      ,total_interest
      ,is_addishop_referral
      ,addishop_channel
      ,is_addishop_referral_paid
      ,shop_used_grouped_config
      ,addishop_opt_in_date
      ,addishop_opt_out_date
      ,lead_gen_fee_rate
      ,CASE WHEN country_code = 'CO' THEN requested_amount_co * COALESCE(lead_gen_fee_rate,0) 
            ELSE gmv * COALESCE(lead_gen_fee_rate,0) 
       END AS lead_gen_fee_amount
      ,collection_fee_income_amount AS expected_collection_fee_amount
      ,fx_rate
      ,lbl
      ,term
      ,client_type
      ,segment
      ,journey_name
      ,synthetic_channel
      ,credit_policy_name   
      ,original_product
      ,processed_product
      ,synthetic_product_category
      ,synthetic_product_subcategory
      ,santander_branched
      ,santander_origination  
      ,pd_calculation_method     
      ,ally_slug
      ,store_slug
      ,ally_vertical
      ,ally_brand 
      ,ally_cluster
      ,dq31_at_31_upb
      ,dq31_at_31_opb
      ,dq31_at_31_date
      ,expected_final_losses
      ,CASE WHEN country_code = 'CO' THEN COALESCE(expected_final_losses,0) / approved_amount 
            ELSE 0
       END AS expected_loss_rate
      ,payment_date
      , report_term
      ,payment_term
      ,diff_payment_date
FROM transformed_table
)
, final_table AS (
SELECT country_code
      ,loan_id
      ,application_id
      ,order_id
      ,client_id
      ,origination_date
      ,origination_date_local
      ,origination_hour_local
      ,origination_minute_local
      ,approved_amount
      ,approved_amount_filtered_for_losses
      ,requested_amount
      ,gmv
      ,CAST(guarantee_rate AS FLOAT) AS guarantee_rate
      ,CASE WHEN synthetic_product_category = 'GRANDE' THEN guarantee_amount * (1/1.19) * 0.9 * (1 - expected_loss_rate/2) 
            ELSE guarantee_amount * (1/1.19) * 0.9 * (1 - expected_loss_rate) 
       END AS guarantee_amount
      ,CAST(interest_rate AS FLOAT) AS interest_rate
      ,CASE WHEN synthetic_product_category = 'GRANDE' THEN total_interest * (1 - expected_loss_rate/2) 
       ELSE total_interest * (1 - expected_loss_rate) 
       END AS total_interest
      ,is_addishop_referral
      , addishop_channel
      ,is_addishop_referral_paid
      ,shop_used_grouped_config
      ,addishop_opt_in_date
      , addishop_opt_out_date
      ,lead_gen_fee_rate
      ,lead_gen_fee_amount
      ,expected_collection_fee_amount
      ,ally_mdf
      ,mdf_amount
      ,COALESCE(lead_gen_fee_amount,0) + COALESCE(mdf_amount,0) AS merchant_revenue
      ,fx_rate
      ,lbl
      ,term
      ,client_type
      ,segment
      ,journey_name
      ,synthetic_channel
      ,credit_policy_name   
      ,original_product
      ,processed_product
      ,synthetic_product_category
      ,synthetic_product_subcategory
      ,santander_branched
      ,santander_origination
      ,pd_calculation_method
      ,ally_slug
      ,store_slug
      ,ally_vertical
      ,ally_brand
      ,ally_cluster
      ,dq31_at_31_upb
      ,dq31_at_31_opb
      ,dq31_at_31_date
      ,expected_final_losses
      ,payment_date AS loan_payment_date
      ,report_term AS ally_report_term
      ,payment_term AS ally_payment_term
      ,diff_payment_date AS ally_diff_payment_date
FROM amounts_table
)
SELECT country_code
      ,loan_id
      ,application_id
      ,order_id
      ,client_id
      ,origination_date
      ,origination_date_local
      ,origination_hour_local
      ,origination_minute_local
      ,approved_amount
      ,approved_amount_filtered_for_losses
      ,requested_amount
      ,gmv
      ,guarantee_rate
      ,guarantee_amount
      ,interest_rate
      ,total_interest
      ,expected_collection_fee_amount
      ,COALESCE(total_interest,0) + COALESCE(guarantee_amount,0) + COALESCE(expected_collection_fee_amount,0) AS consumer_revenue
      ,is_addishop_referral
      , addishop_channel
      ,is_addishop_referral_paid
      ,shop_used_grouped_config
      ,addishop_opt_in_date
      , addishop_opt_out_date
      ,lead_gen_fee_rate
      ,lead_gen_fee_amount
      ,ally_mdf
      ,mdf_amount
      ,merchant_revenue
      ,COALESCE(total_interest,0) + COALESCE(guarantee_amount,0) + COALESCE(expected_collection_fee_amount,0) + COALESCE(merchant_revenue,0) AS total_revenue
      ,fx_rate
      ,lbl
      ,term
      ,client_type
      ,segment
      ,journey_name
      ,synthetic_channel
      ,credit_policy_name   
      ,original_product
      ,processed_product
      ,synthetic_product_category
      ,synthetic_product_subcategory
      ,santander_branched
      ,santander_origination
      ,pd_calculation_method
      ,ally_slug
      ,store_slug
      ,ally_vertical
      ,ally_brand
      ,ally_cluster
      ,dq31_at_31_upb
      ,dq31_at_31_opb
      ,dq31_at_31_date
      ,expected_final_losses
      ,loan_payment_date
      ,ally_report_term
      ,ally_payment_term
      ,ally_diff_payment_date
      ,NOW() AS ingested_at
      ,to_timestamp('2022-01-01') AS updated_at
FROM final_table