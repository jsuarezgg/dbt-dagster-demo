{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

with apps as (

  select *
  from {{ ref('rmt_apps_gold_staging_co') }}

)

, kyc_addresses_last as (
    select application_id,
           ocurred_on,
           bureau_address_city as id_exp_city,
           row_number() over (partition by application_id order by ocurred_on desc) as rn
    from {{ source('silver_live', 'f_kyc_bureau_contact_info_addresses_co_logs') }}
    where bureau_address_city is not null
)

, kyc_id_region as (
    select
      ka.application_id
      --,a.id_exp_city as old_exp_city
      ,ech.city_name as id_exp_city
      ,ech.region_name as id_exp_region
      ,row_number() over(partition by ka.application_id order by ka.application_id) as rn
    from kyc_addresses_last ka
    left join {{ source('gold', 'expedition_city_homologation_co') }} ech on ka.id_exp_city = ech.id_exp_city
    where ka.rn = 1
)

, kyc_id_region_v2 as (
    select
      aci.application_id
      --,a.id_exp_city as old_exp_city
      ,ech.city_name as id_exp_city
      ,ech.region_name as id_exp_region
      ,row_number() over(partition by aci.application_id order by aci.application_id) as rn
    from {{ ref('rmt_application_city_information_co') }} aci
    left join {{ source('gold', 'expedition_city_homologation_co') }} ech on aci.id_expedition = ech.id_exp_city
)

, kyc_id_region_unified as (
    select coalesce(kyc_a.application_id, kyc_b.application_id) as application_id,
             coalesce(kyc_a.id_exp_city, kyc_b.id_exp_city) as id_exp_city,
             coalesce(kyc_a.id_exp_region, kyc_b.id_exp_region) as id_exp_region
    from kyc_id_region_v2 kyc_a
    full outer join kyc_id_region kyc_b
    on kyc_a.application_id = kyc_b.application_id
    where kyc_a.rn = 1 and kyc_b.rn = 1
)

-- Create buckets for different dimensions
, grouping_buckets as (
    select
      application_id
      ,case
        when days_past_due > 120 then '120 +'
        when days_past_due > 90 then '91 to 120'
        when days_past_due > 60 then '61 to 90'
        when days_past_due > 30 then '31 to 60'
        when days_past_due > 15 then '16 to 30'
        when days_past_due > 7 then '7 to 15'
        when days_past_due > 0 then '1 to 6'
        when days_past_due = 0 and first_payment_date >= current_status_calc_date then 'Current'
        when first_payment_date < current_status_calc_date then 'Before First Payment'
        end as dq_buckets
  from apps
) ,

-- clients_blacklisted AS (
--     SELECT
--         ppd.client_id,
--         CASE WHEN bld.id_number IS NOT NULL
--                 OR ble.email IS NOT NULL
--                 OR blp.cell_phone_number IS NOT NULL
--                 OR r.client_id IS NOT NULL THEN True ELSE False END AS client_blacklisted
--     FROM {{ source('cur', 'pii_clients') }} ppd
--     LEFT JOIN {{ source('cur', 'kyc_blacklisted_documents') }} bld
--         ON ppd.id_number = bld.id_number
--     LEFT JOIN {{ source('cur', 'kyc_blacklisted_emails') }} ble
--         ON ppd.last_application_email = ble.email
--     LEFT JOIN {{ source('cur', 'kyc_phone_blacklist') }} blp
--         ON ppd.last_application_cellphone = blp.cell_phone_number
--     LEFT JOIN {{ source('risk', 'comms_exclusion') }} r
--         ON ppd.client_id = r.client_id
-- ),
loan_cancellations AS (
    SELECT
        loan_id,
        loan_cancellation_order_date,
        loan_cancellation_reason,
        CASE
            WHEN loan_cancellation_reason IN ('FRAUD','ALLY_BAD_PRACTICE','CLIENT_DEATH') THEN TRUE
        END AS non_refunded_cancellation,
        COALESCE(loan_cancellation_type, 'TOTAL') AS loan_cancellation_type
    FROM {{ source('silver_live', 'f_loan_cancellations_v2_co') }}
    WHERE custom_loan_cancellation_status NOT ilike '%V2_CANCELLATION_ANNULLED%'

),
charge_off_report AS (
    SELECT
        loan_id,
        charge_off_date,
        'CREDIT' AS charge_off_reason
    FROM {{ ref('f_snc_charge_off_report_co') }}
    WHERE is_charge_off IS TRUE
    UNION ALL
    SELECT
        loan_id,
        from_utc_timestamp(loan_cancellation_order_date, 'America/Bogota')::date AS charge_off_date,
        'NON_REFUNDED_CANCELLATION' AS charge_off_reason
    FROM {{ source('silver_live', 'f_loan_cancellations_v2_co') }}
    WHERE loan_cancellation_reason IN ('FRAUD','ALLY_BAD_PRACTICE','CLIENT_DEATH')
        AND custom_loan_cancellation_status = 'V2_CANCELLATION_PROCESSED'
        AND loan_id NOT IN (SELECT loan_id FROM {{ ref('f_snc_charge_off_report_co') }} WHERE is_charge_off IS TRUE)
),
condonation_reasons AS (
    SELECT
        loan_id,
        sum(CASE WHEN condonation_reason = 'COLLECTIONS' THEN condoned_amount END) AS condoned_amount_by_collections,
        sum(CASE WHEN condonation_reason = 'FGA_CLAIM' THEN condoned_amount END) AS condoned_amount_by_fga_claim,
        sum(CASE WHEN condonation_reason = 'LOAN_RECAST' THEN condoned_amount END) AS condoned_amount_by_loan_recast,
        sum(CASE WHEN condonation_reason = 'OTHER_REASONS'THEN condoned_amount END) AS condoned_amount_by_other_reasons,
        min(CASE WHEN condonation_reason = 'COLLECTIONS' THEN condonation_date END) AS condonation_date_by_collections,
        min(CASE WHEN condonation_reason = 'FGA_CLAIM' THEN condonation_date END) AS condonation_date_by_fga_claim,
        min(CASE WHEN condonation_reason = 'LOAN_RECAST' THEN condonation_date END) AS condonation_date_by_loan_recast,
        min(CASE WHEN condonation_reason = 'OTHER_REASONS' THEN condonation_date END) AS condonation_date_by_other_reasons
    FROM {{ ref('dm_condonations') }}
    WHERE country = 'CO'
        AND condonation_reason IS NOT NULL
    GROUP BY 1
),
fully_settled_paid_loans AS (
    SELECT
        *
    FROM {{ ref('rmt_fully_settled_paid_loans_co') }}
),
---##<CONSOLIDATE EVERYTHING>##---
consolidated as (
    SELECT DISTINCT
      apps.application_id
      ,apps.loan_id
      ,apps.loan_originated
      ,apps.prospect_id
      ,apps.store_user_id
      ,altr.refinanced_by_origination_of_loan_id
      ,apps.application_date_time --Application date with time
      ,apps.application_date_time_local
      ,apps.d_vintage --Application day
      ,apps.w_vintage --Application week
      ,apps.m_vintage --Application month
      ,apps.q_vintage --Application quarter
      ,apps.product
      ,apps.application_product
      ,apps.synthetic_product_category
      ,apps.synthetic_product_subcategory
      ,apps.amount --Approved/Requested amount
      ,apps.approved_amount
      ,apps.remaining_addicupo
      ,apps.requested_amount
      ,apps.amount_before_discount
      ,apps.preapproval_amount
      ,apps.preapproval_amount_approved
      ,apps.term
      ,apps.interest_rate
      ,coalesce(mkplc_bl.synthetic_origination_origination_mdf, apps.mdf, apps.mdf_bnpn) as mdf
      ,mkplc_bl.synthetic_origination_marketplace_purchase_fee
      ,mkplc_bl.synthetic_origination_marketplace_purchase_fee_amount
      ,apps.fga
      ,apps.fga_effective
      ,apps.ally_slug
      ,apps.ally_name
      ,apps.store_name
      ,apps.ally_brand
      ,apps.ally_vertical
      ,apps.ally_state
      ,apps.ally_is_terminated
      ,apps.ally_terminated_date --last_origination_date
      ,apps.ally_region      
      ,kyc_id.id_exp_city as id_exp_city
      ,kyc_id.id_exp_region as id_exp_region
      ,apps.addi_pd_multiplied
      ,apps.addi_pd
      ,apps.final_pd_score
      ,apps.pd_model_name
      ,apps.original_pd_score
      ,apps.pd_multiplier
      ,apps.final_fpd_score
      ,apps.pd_no_multipliers
      ,apps.bureau_pd
      ,apps.credit_score
      ,apps.credit_score_name      
      ,apps.journey_name
      ,apps.journey_stage_name
      ,apps.stage
      ,apps.final_event_type
      ,apps.journey_stages
      ,apps.stages
      ,apps.evaluation_type
      ,apps.credit_status_reason
      ,apps.credit_policy_name
      ,apps.low_balance_loan
      ,apps.campaign_id
      ,apps.marketing_channel
      ,apps.prospect_age_range
      ,apps.prospect_age_avg
      ,apps.learning_population
      ,apps.ia_loan
      ,apps.client_type --Client type extracted from database
      ,apps.client_type_lc
      -- IDV Data
      ,apps.possible_idv
      ,apps.bypass
      ,apps.idv_started
      ,apps.idv_prev_approved
      ,apps.idv_approved
      ,apps.idv_status
      ,apps.idv_status_final_event
      ,apps.preapproval_client
      ,apps.preapproval_application
      ,apps.application_channel
      ,apps.imply_agg
      ,apps.app_process_synthetic_product_order
      ,apps.application_number
      ,apps.apps_no_preapp
      ,apps.loan_number
      ,apps.classification_at_origination
      ,apps.financial_experience
      ,apps.bad_loan
      ,CAST(apps.lead_gen_fee AS FLOAT) AS lead_gen_fee
      ,apps.is_flex
      ,apps.total_interest
      ,apps.guarantee_rate
      ,apps.guarantee_provider_with_default
      --Performance data
      ,apps.current_status_calc_date
      ,apps.mob
      ,apps.is_fully_paid
      ,apps.fully_paid_date
      ,apps.is_fully_paid AS is_fully_settled
      ,apps.fully_paid_date AS fully_settled_date
      ,CASE
        WHEN fpl.fully_paid_date IS NOT NULL THEN TRUE
        ELSE FALSE
       END AS is_fully_paid_v2
      ,fpl.fully_paid_date AS is_fully_paid_date_v2
      ,apps.n_previous_fully_paid_loans
      ,apps.is_cancelled
      ,cast(dflc.loan_cancellation_order_date as date) as cancellation_date
      ,dflc.loan_cancellation_reason as cancellation_reason
      ,dflc.loan_cancellation_type as loan_cancellation_type
      ,dflc.non_refunded_cancellation as non_refunded_cancellation
      ,apps.fraud_write_off
      ,CASE
        WHEN cor.charge_off_date IS NOT NULL THEN TRUE
        WHEN apps.days_past_due > 120 IS TRUE THEN TRUE
        WHEN dm_cond.condoned_amount_by_fga_claim > 0 THEN TRUE
       END AS is_charge_off
      ,CASE
        WHEN cor.charge_off_reason IS NOT NULL THEN cor.charge_off_reason
        WHEN dm_cond.condonation_date_by_fga_claim IS NOT NULL THEN 'FGA_CLAIM'
       END AS charge_off_reason
      ,CASE
        WHEN cor.charge_off_date IS NOT NULL THEN cor.charge_off_date
        WHEN dm_cond.condonation_date_by_fga_claim IS NOT NULL THEN dm_cond.condonation_date_by_fga_claim
       END AS charge_off_date
      ,apps.approved_amount -
        COALESCE(GREATEST(dm_cond.condoned_amount_by_loan_recast, (CASE WHEN dflc.non_refunded_cancellation IS TRUE THEN apps.approved_amount END)), 0) AS net_opb
      ,apps.approved_amount -
        COALESCE(GREATEST(dm_cond.condoned_amount_by_loan_recast, (CASE WHEN dflc.non_refunded_cancellation IS TRUE THEN apps.approved_amount END)), 0) -
        apps.total_principal_paid AS outstanding_principal_balance
      ,apps.paid_installments
      ,apps.total_principal_paid
      ,apps.days_past_due
      ,apps.unpaid_principal
      ,apps.unpaid_interest
      ,apps.unpaid_guarantee
      ,apps.principal_condoned
      ,apps.interest_condoned
      ,apps.guarantee_condoned
      ,dm_cond.condoned_amount_by_collections
      ,dm_cond.condoned_amount_by_fga_claim
      ,dm_cond.condoned_amount_by_loan_recast
      ,dm_cond.condoned_amount_by_other_reasons
      --Client metrics
      ,apps.max_days_past_due
      ,apps.installment_paid_in_delinquency_n
      ,apps.installment_paid_in_delinquency_proportion
      ,apps.n_active_days
      ,apps.first_contact
      ,apps.has_contacted_before
      --GROUPING
      ,gb.dq_buckets
      --Dates where loan reached maturity
      ,apps.first_payment_date
      ,met.FP_date_plus_1_day
      ,met.FP_date_plus_5_day
      ,met.FP_date_plus_10_day
      ,met.FP_date_plus_15_day
      ,met.FP_date_plus_1_month
      ,met.FP_date_plus_2_month
      ,met.FP_date_plus_3_month
      ,met.FP_date_plus_4_month
      ,met.FP_date_plus_5_month
      ,met.FP_date_plus_6_month
      ,met.FP_date_plus_7_month
      ,met.FP_date_plus_8_month
      ,met.FP_date_plus_9_month
      ,met.FP_date_plus_10_month
      ,met.FP_date_plus_11_month
      ,met.FP_date_plus_12_month
      ,met.FP_date_plus_13_month
      ,met.FP_date_plus_14_month
      ,met.FP_date_plus_15_month
      ,met.FP_date_plus_16_month
      ,met.FP_date_plus_17_month
      ,met.FP_date_plus_18_month
      ,met.FP_date_plus_19_month
      ,met.FP_date_plus_20_month
      ,met.FP_date_plus_21_month
      ,met.FP_date_plus_22_month
      ,met.FP_date_plus_23_month
      ,met.FP_date_plus_24_month
      --Number of loans delinquent at each maturity
      ,met.DPD_plus_1_day
      ,met.DPD_plus_5_day
      ,met.DPD_plus_10_day
      ,met.DPD_plus_15_day
      ,met.DPD_plus_1_month
      ,met.DPD_plus_2_month
      ,met.DPD_plus_3_month
      ,met.DPD_plus_4_month
      ,met.DPD_plus_5_month
      ,met.DPD_plus_6_month
      ,met.DPD_plus_7_month
      ,met.DPD_plus_8_month
      ,met.DPD_plus_9_month
      ,met.DPD_plus_10_month
      ,met.DPD_plus_11_month
      ,met.DPD_plus_12_month
      ,met.DPD_plus_13_month
      ,met.DPD_plus_14_month
      ,met.DPD_plus_15_month
      ,met.DPD_plus_16_month
      ,met.DPD_plus_17_month
      ,met.DPD_plus_18_month
      ,met.DPD_plus_19_month
      ,met.DPD_plus_20_month
      ,met.DPD_plus_21_month
      ,met.DPD_plus_22_month
      ,met.DPD_plus_23_month
      ,met.DPD_plus_24_month
      --UPB of delinquent loans
      ,met.UPB_plus_1_day
      ,met.UPB_plus_5_day
      ,met.UPB_plus_10_day
      ,met.UPB_plus_15_day
      ,met.UPB_plus_1_month
      ,met.UPB_plus_2_month
      ,met.UPB_plus_3_month
      ,met.UPB_plus_4_month
      ,met.UPB_plus_5_month
      ,met.UPB_plus_6_month
      ,met.UPB_plus_7_month
      ,met.UPB_plus_8_month
      ,met.UPB_plus_9_month
      ,met.UPB_plus_10_month
      ,met.UPB_plus_11_month
      ,met.UPB_plus_12_month
      ,met.UPB_plus_13_month
      ,met.UPB_plus_14_month
      ,met.UPB_plus_15_month
      ,met.UPB_plus_16_month
      ,met.UPB_plus_17_month
      ,met.UPB_plus_18_month
      ,met.UPB_plus_19_month
      ,met.UPB_plus_20_month
      ,met.UPB_plus_21_month
      ,met.UPB_plus_22_month
      ,met.UPB_plus_23_month
      ,met.UPB_plus_24_month
      --Condonation by maturity    
      ,met.condoned_amount_plus_1_day
      ,met.condoned_amount_plus_5_day
      ,met.condoned_amount_plus_10_day
      ,met.condoned_amount_plus_15_day
      ,met.condoned_amount_1_month
      ,met.condoned_amount_2_month
      ,met.condoned_amount_3_month
      ,met.condoned_amount_4_month
      ,met.condoned_amount_5_month
      ,met.condoned_amount_6_month
      ,met.condoned_amount_7_month
      ,met.condoned_amount_8_month
      ,met.condoned_amount_9_month
      ,met.condoned_amount_10_month
      ,met.condoned_amount_11_month
      ,met.condoned_amount_12_month
      ,met.condoned_amount_13_month
      ,met.condoned_amount_14_month
      ,met.condoned_amount_15_month
      ,met.condoned_amount_16_month
      ,met.condoned_amount_17_month
      ,met.condoned_amount_18_month
      ,met.condoned_amount_19_month
      ,met.condoned_amount_20_month
      ,met.condoned_amount_21_month
      ,met.condoned_amount_22_month
      ,met.condoned_amount_23_month
      ,met.condoned_amount_24_month
      --<Additional Variables>--
      , apps.funding_partner_ownership
      , apps.funding_partner_sale_date
      --<Imply Variables>--
      , apps.background_check_co_in
      , apps.background_check_co_out
      , apps.device_information_in
      , apps.device_information_out
      , apps.fraud_check_co_in
      , apps.fraud_check_co_out
      --, apps.fraud_check_rc_co_in
      --, apps.fraud_check_rc_co_out
      , apps.loan_proposals_co_in
      , apps.loan_proposals_co_out
      --, apps.loan_proposals_santander_co_in
      --, apps.loan_proposals_santander_co_out
      , apps.preconditions_in
      , apps.preconditions_out
      , apps.underwriting_in
      , apps.underwriting_out
      , apps.loan_acceptance_in
      , apps.loan_acceptance_out
      , apps.loan_acceptance_v2_in
      , apps.loan_acceptance_v2_out
      , apps.identity_in
      , apps.identity_out
      , apps.additional_information_co_in
      , apps.additional_information_co_out
      , apps.basic_identity_co_in
      , apps.basic_identity_co_out
      , apps.cellphone_validation_co_in
      , apps.cellphone_validation_co_out
      , apps.personal_information_co_in
      , apps.personal_information_co_out
      , apps.preapproval_summary_co_in
      , apps.preapproval_summary_co_out
      , apps.privacy_policy_stage_in
      , apps.privacy_accepted_co_out
      , apps.privacy_expiration_date_co_out
      , apps.privacy_first_name_co_out
      , apps.privacy_policy_co_in
      , apps.privacy_policy_co_out
      , apps.privacy_policy_v2_co_in
      , apps.privacy_policy_v2_co_out
      , apps.psychometric_assessment_co_in
      , apps.psychometric_assessment_co_out
      , apps.work_information_co_in
      , apps.work_information_co_out
      , apps.is_attributable_transaction
      , apps.application_process_id
      , apps.ocurred_on_date_funnel
      , apps.unpaid_collection_fees
      , apps.total_collection_fees_paid
      , apps.total_collection_fees_condoned
    from apps
    left join {{ ref('rmt_metrics_co') }} met on apps.loan_id = met.loan_id
    left join {{ ref('bl_originations_marketplace_suborders_to_originations_co') }} mkplc_bl on apps.application_id = mkplc_bl.application_id
    left join {{ source('silver_live', 'f_approval_loans_to_refinance_co') }} AS altr on apps.loan_id = altr.loan_id
    left join grouping_buckets gb on apps.application_id = gb.application_id
    --<Additional Variables>--
    left join kyc_id_region_unified kyc_id on apps.application_id = kyc_id.application_id
    left join loan_cancellations dflc on apps.loan_id = dflc.loan_id
    --left join clients_blacklisted cbl on apps.prospect_id = cbl.client_id
    left join charge_off_report cor on apps.loan_id = cor.loan_id
    left join condonation_reasons dm_cond on apps.loan_id = dm_cond.loan_id
    left join fully_settled_paid_loans fpl on apps.loan_id = fpl.loan_id
)

select * from consolidated
