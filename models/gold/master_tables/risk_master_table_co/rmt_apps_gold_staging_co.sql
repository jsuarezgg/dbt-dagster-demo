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
  from {{ ref('rmt_apps_silver_staging_co') }}

),
apps_no_preapp AS (
    SELECT
        application_id,
        apps_no_preapp
    FROM {{ ref('bl_application_preapproval_proxy') }}
    WHERE apps_no_preapp IS NOT NULL
),
f_allies_product_policies_co AS (
    SELECT
        *
    FROM {{ ref('f_allies_product_policies_co') }}
),
consolidated as (
    select distinct
      apps.application_id
      ,apps.loan_id
      ,apps.loan_originated
      ,apps.prospect_id
      ,apps.store_user_id
      ,cast(apps.general_date as timestamp) as application_date_time --Application date with time
      ,from_utc_timestamp(apps.general_date, 'America/Bogota') as application_date_time_local
      ,from_utc_timestamp(apps.general_date, 'America/Bogota')::date as d_vintage --Application day
      ,trunc(from_utc_timestamp(apps.general_date, 'America/Bogota'),'week') as w_vintage --Application week
      ,date_format(from_utc_timestamp(apps.general_date, 'America/Bogota'), "yyyy-MM") as m_vintage --Application month
      ,date_format(from_utc_timestamp(apps.general_date, 'America/Bogota'),"yyyy-QQ") as q_vintage --Application quarter
      ,apps.product
      ,apps.application_product
      ,apps.synthetic_product_category
      ,apps.synthetic_product_subcategory
      ,apps.general_amount as amount --Approved/Requested amount
      ,apps.approved_amount
      ,apps.remaining_addicupo
      ,apps.requested_amount
      ,apps.amount_before_discount
      ,apps.preapproval_amount
      ,app_pr.preapproval_amount as preapproval_amount_approved
      ,cast(apps.term as int) as term
      ,cast(apps.interest_rate as double) as interest_rate
      ,cast(apps.mdf as double) as mdf
      ,cast(apol.origination_mdf as double) as mdf_bnpn
      ,apps.fga
      ,apps.fga_effective
      ,apps.ally_slug
      ,apps.ally_name
      ,apps.store_name
      ,apps.ally_brand
      ,apps.ally_vertical
      ,apps.ally_state
      ,apps.ally_is_terminated
      ,ta.ally_terminated_date --last_origination_date
      ,apps.ally_region      
      ,case when apps.general_date::date >= current_date() - interval '1 day'
              then cast(pdm.final_pd_score as double)
            else cast(coalesce(pdm.final_pd_score, apps.addi_pd) as double)
              end as addi_pd_multiplied
      ,case when apps.general_date::date >= current_date() - interval '1 day'
              then cast(pdm.pd_no_multipliers as double)
            else cast(coalesce(pdm.pd_no_multipliers, apps.addi_pd) as double)
              end as addi_pd
      ,cast(pdm.final_pd_score as double) as final_pd_score
      ,pdm.pd_model_name
      ,cast(pdm.original_pd_score as double) as original_pd_score
      ,pdm.pd_multiplier
      ,cast(pdm.final_fpd_score as double) as final_fpd_score
      ,cast(pdm.pd_no_multipliers as double) as pd_no_multipliers
      ,cast(coalesce(pdm.bureau_pd, apps.bureau_pd) as double) as bureau_pd
      ,case when apps.general_date::date >= current_date() - interval '1 day'
              then cast(pdm.credit_score as double)
            else cast(coalesce(pdm.credit_score, apps.credit_score) as double)
              end as credit_score
      ,coalesce(pdm.credit_score_name, apps.credit_score_name) as credit_score_name      
      ,apps.journey_name
      ,apps.journey_stage_name
      ,apps.stage
      ,apps.final_event_type
      ,apps.journey_stages
      ,apps.stages
      ,apps.evaluation_type
      ,apps.credit_status_reason
      ,case when apps.general_date::date >= current_date() - interval '1 day'
              then pdm.credit_policy_name
            else coalesce(pdm.credit_policy_name, apps.credit_policy_name)
              end as credit_policy_name
      ,apps.low_balance_loan
      ,apps.campaign_id
      ,substring(apps.campaign_id,14,3) as marketing_channel
      ,apps.learning_population
      ,case when apps.learning_population is true 
                  or pdm2.application_id is not null then true else false end as ia_loan
      ,apps.client_type --Client type extracted from database
      ,case when calc.loans_cancelled_flag = 'All Prev Loans Cancelled' then 'PROSPECT'
                else apps.client_type end as client_type_lc
      -- IDV Data
      ,apps.possible_idv
      ,apps.bypass
      ,apps.idv_started
      ,case 
        when hia.first_idv_approval_date < apps.general_date then hia.idv_approved else 0 
        end as idv_prev_approved
      ,apps.idv_approved
      ,apps.idv_status
      ,apps.idv_status_final_event
      ,case 
        when apps.application_date > first_pr.min_preapproval_date then 'Preapproval'
        else 'Not Preapproval'
        end as preapproval_client
      ,case 
        when ( apps.application_date > app_pr.application_date
                and apps.requested_amount <= app_pr.preapproval_amount
                and app_pr.ally_preapproval like ('%addi%') ) then 'ADDI Preapproval'
        when ( apps.application_date > app_pr.application_date
                and apps.requested_amount <= app_pr.preapproval_amount
                and app_pr.ally_preapproval is not null ) then 'Widget' 
        when ( apps.application_date > app_pr.application_date
                and apps.requested_amount <= app_pr.preapproval_amount ) then 'ADDI Preapproval'
        else 'Not Preapproval'
        end as preapproval_application
      ,apps.application_channel
      ,apps.imply_agg
      ,apps.app_process_synthetic_product_order
      ,apps.application_number
      ,apps.loan_number
      ,case
        when apps.loan_number = 1 then 'New Client'
        when apps.loan_number > 1 
          and (dm_cs.first_fully_paid_loan_date is null or dm_cs.first_fully_paid_loan_date > apps.general_date) 
          and dm_cs.first_installment_paid_date <= apps.general_date
          then 'Returning Client - Paid Installment'
        when apps.loan_number > 1 
          and dm_cs.first_fully_paid_loan_date <= apps.general_date 
          then 'Returning Client - Paid Loan'
        when apps.loan_number > 1 
          and (dm_cs.first_fully_paid_loan_date is null or dm_cs.first_fully_paid_loan_date > apps.general_date) 
          and (dm_cs.first_installment_paid_date is null or dm_cs.first_installment_paid_date > apps.general_date) 
          then 'Returning Client - No Payments'
        end as classification_at_origination
      ,apps.financial_experience
      ,apps.bad_loan
      ,apps.lead_gen_fee
      ,apps.is_flex
      ,cast(apps.total_interest as double) as total_interest
      ,cast(apps.guarantee_rate as double) as guarantee_rate
      ,apps.guarantee_provider_with_default
      --Performance data
      ,apps.calculation_date as current_status_calc_date
      ,floor(months_between(CURRENT_DATE(), apps.general_date::date)) AS mob
      ,apps.is_fully_paid
      ,apps.fully_paid_date
      ,apps.is_fully_paid AS is_fully_settled
      ,apps.fully_paid_date AS fully_settled_date
      ,apps.n_previous_fully_paid_loans
      ,case when apps.cancellation_reason is not null then true else false end as is_cancelled
      ,case when apps.cancellation_reason in ('Fraud - Write Off', 'Write Off', 'fraud - Write Off') then apps.approved_amount end as fraud_write_off
      ,apps.paid_installments
      ,apps.total_principal_paid
      ,apps.days_past_due
      ,apps.unpaid_principal
      ,apps.unpaid_interest
      ,apps.unpaid_guarantee
      ,apps.principal_condoned
      ,apps.interest_condoned
      ,apps.guarantee_condoned
      --Client metrics
      ,dm_cs.max_days_past_due
      ,dm_cs.installment_paid_in_delinquency_n
      ,dm_cs.installment_paid_in_delinquency_proportion
      ,dm_cs.n_active_days
      ,dm_cs.first_contact
      ,case when dm_cs.first_contact < apps.general_date then True else False end as has_contacted_before
      ,dm_cs.prospect_age_range
      ,dm_cs.prospect_age_avg
      --Dates where loan reached maturity
      ,apps.first_payment_date
      --<Additional Variables>--
      , case when lo.sale_date is not null then lo.loan_ownership else "ADDI" end as funding_partner_ownership
      , lo.sale_date::date as funding_partner_sale_date
      --<Funnel boolean stage variables>--
      , funnel_dm.background_check_co_in
      , funnel_dm.background_check_co_out
      , funnel_dm.device_information_co_in AS device_information_in
      , funnel_dm.device_information_co_out AS device_information_out
      , funnel_dm.fraud_check_co_in
      , funnel_dm.fraud_check_co_out
      , funnel_dm.loan_proposals_co_in
      , funnel_dm.loan_proposals_co_out
      , funnel_dm.preconditions_co_in as preconditions_in
      , funnel_dm.preconditions_co_out as preconditions_out
      , funnel_dm.underwriting_co_in as underwriting_in
      , funnel_dm.underwriting_co_out as underwriting_out
      , funnel_dm.loan_acceptance_co_in as loan_acceptance_in
      , funnel_dm.loan_acceptance_co_out as loan_acceptance_out
      , funnel_dm.loan_acceptance_v2_co_in as loan_acceptance_v2_in
      , funnel_dm.loan_acceptance_v2_co_out as loan_acceptance_v2_out
      , funnel_dm.identity_verification_co_in as identity_in
      , funnel_dm.identity_verification_co_out as identity_out
      , funnel_dm.additional_information_co_in
      , funnel_dm.additional_information_co_out
      , funnel_dm.basic_identity_co_in
      , funnel_dm.basic_identity_co_out
      , funnel_dm.cellphone_validation_co_in
      , funnel_dm.cellphone_validation_co_out
      , funnel_dm.personal_information_co_in
      , funnel_dm.personal_information_co_out
      , funnel_dm.preapproval_summary_co_in
      , funnel_dm.preapproval_summary_co_out
      , funnel_dm.privacy_policy_stage_in
      , funnel_dm.privacy_accepted_co_out
      , funnel_dm.privacy_expiration_date_co_out
      , funnel_dm.privacy_first_name_co_out
      , funnel_dm.privacy_policy_co_in
      , funnel_dm.privacy_policy_co_out
      , funnel_dm.privacy_policy_v2_co_in
      , funnel_dm.privacy_policy_v2_co_out
      , funnel_dm.psychometric_assessment_co_in
      , funnel_dm.psychometric_assessment_co_out
      , funnel_dm.work_information_co_in
      , funnel_dm.work_information_co_out
      , apps.is_attributable_transaction
      , apps.application_process_id
      , apps.ocurred_on_date_funnel
      , apps.unpaid_collection_fees
      , apps.total_collection_fees_paid
      , apps.total_collection_fees_condoned
      , app_no_pr.apps_no_preapp
    from apps
    left join {{ ref('rmt_application_preapproval_co') }} app_pr
      on apps.prospect_id = app_pr.prospect_id 
      and apps.application_date between app_pr.application_date and app_pr.preapproval_expiration_date 
      and rn = 1
    left join {{ ref('rmt_first_preapproval_date_co') }} first_pr on apps.prospect_id = first_pr.client_id
    --left join {{ ref('rmt_metrics_co') }} met on apps.loan_id = met.loan_id
    --left join grouping_buckets gb on apps.application_id = gb.application_id
    left join {{ ref('dm_customer_segmentation') }} dm_cs on apps.prospect_id = dm_cs.client_id and dm_cs.country = 'CO'
    --left join pii_information pii on apps.application_id = pii.application_id
    --left join apps_no_preapp app_no_pr on apps.application_id = app_no_pr.application_id
    --<Additional Variables>--
    left join {{ ref('rmt_historic_idv_approved_co') }} hia on apps.prospect_id = hia.client_id
    left join {{ ref('rmt_terminated_allies_co') }} ta on apps.ally_slug = ta.ally_slug
    --left join kyc_id_region_unified kyc_id on apps.application_id = kyc_id.application_id
    left join {{ ref('loan_ownership_co') }} lo on apps.loan_id = lo.loan_id
    left join addi_prod.gold.rmt_pd_models_co pdm on apps.application_id = pdm.application_id
    left join addi_prod.gold.rmt_pd_models_co pdm2 on apps.application_id=pdm2.application_id and pdm2.final_pd_score < pdm2.original_pd_score --IA loans have a lower final PD
    LEFT JOIN {{ ref('dm_application_process_funnel_co') }} AS funnel_dm ON funnel_dm.application_process_id = apps.application_process_id
    --left join loan_cancellations dflc on apps.loan_id = dflc.loan_id
    --left join charge_off_report cor on apps.loan_id = cor.loan_id
    --left join condonation_reasons dm_cond on apps.loan_id = dm_cond.loan_id
    left join {{ ref('rmt_client_all_loans_cancelled') }} calc on apps.loan_id = calc.loan_id
    left join apps_no_preapp app_no_pr on apps.application_id = app_no_pr.application_id
    left join f_allies_product_policies_co apol on apps.application_id = apol.application_id
        and apol.type = 'ADDI_BNPN' and apol.product = 'BNPN_CO'
)

select * from consolidated;