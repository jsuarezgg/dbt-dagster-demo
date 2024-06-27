

with apps as (
select a.application_id
      ,orig.loan_id
      ,a.client_id as prospect_id
      ,coalesce(orig.origination_date, orig_bnpn.origination_date) as origination_date
      ,a.application_date
      ,a.campaign_id
      ,coalesce(a.application_date,orig.origination_date,orig_bnpn.origination_date) as general_date
      ,coalesce(orig.approved_amount, orig_bnpn.requested_amount) as approved_amount
      ,a.requested_amount
      ,a.preapproval_amount
      ,case when a.requested_amount_without_discount is not null then a.requested_amount_without_discount else a.requested_amount end as general_amount
      ,case
        when orig.loan_id is not null then true
        else false
        end as loan_originated
      ,lp.term
      ,a.ally_slug
      ,ab.ally_name as ally_name
      ,ab.ally_vertical:['name']['value'] as ally_vertical
      ,a.client_type
      ,a.product
      ,case when
          app_js.journey_stages like '%|down-payment-br|%'
          then 'DP'
          else 'No_DP'
          end as DP_payment
      ,case when a.requested_amount_without_discount is not null and a.requested_amount_without_discount>a.requested_amount then 1 else 0 end as Discounts
      ,case
        when a.requested_amount_without_discount is not null and a.requested_amount_without_discount>a.requested_amount
          then ((a.requested_amount_without_discount-a.requested_amount)::numeric/a.requested_amount_without_discount::numeric)::double
          else 0 end as perc_Discounts
      ,ab.ally_brand:['name']['value'] as ally_brand
      ,udw.credit_policy_name
      ,udw.probability_default_addi as addi_pd
      ,udw.probability_default_bureau as bureau_pd
      ,udw.credit_score
      ,udw.credit_score_name
      ,a.journey_name
      ,orig_ev.journey_stage_name
      ,orig_ev.last_event_name_processed as stage
      ,app_js.journey_stages
      ,app_js.stages
      ,case
        when (a.journey_name ilike '%bnpn%' or a.custom_is_bnpn_branched = True) then True
        else False end as is_bnpn
      ,case when orig_ev.last_event_name_processed = 'PixPaymentReceivedBR' then True else False end as bnpn_received_pay
      ,sc.remaining_addicupo
      ,udw.credit_status_reason
      ,lp.ally_mdf as mdf
      ,from_utc_timestamp(ls.first_payment_date, 'America/Sao_Paulo') as first_payment_date
      ,case
        when a.channel like '%PAY_LINK%' then 'PAY_LINK'
        when a.channel like '%E_COMMERCE%' then 'E_COMMERCE'
        when a.channel ilike '%PRE%APPROVAL%' then 'PREAPPROVAL'
        end as application_channel
      , row_number() over(partition by a.client_id order by a.application_date) as application_number
      , row_number() over (partition by a.client_id, a.journey_name, a.ally_slug, a.application_date::date order by a.application_date desc) as imply_agg
      -- IDV Stats and metrics
      ,CASE
        WHEN (
          (
            idv.application_id is null
            and idv_3.application_id is null
          )
          AND (
            orig.application_id is not null
          )
        ) THEN 1
        ELSE 0
         END AS bypass
      ,CASE
        WHEN (
          (
            idv.application_id is not null
            or idv_3.application_id is not null
          )
        ) THEN 1
        ELSE 0
          END AS possible_idv
      ,CASE WHEN greatest(idv.identitywastarted_at,
                          idv.identityphotosstarted_at,
                          idv.prospectidentityverificationstarted_at,
                          idv_3.idvthirdpartystarted_br_at) is not null
            or (idv.application_id is not null
                or idv_3.application_id is not null)
            THEN 1 ELSE 0 END AS idv_started
      ,CASE
        WHEN (
          --a.stages LIKE '%IdentityWAApproved%'
          --or a.stages LIKE '%IdentityPhotosApproved%'
          idv.identityphotosapproved_at is not null
          or idv.identitywaapproved_at is not null
          or idv_3.idv_tp_custom_last_status = 'APPROVED'
          or idv_3.idvthirdpartyapproved_br_at is not null
        ) THEN 1
        ELSE 0
          END AS idv_approved
      , case
          when greatest(
            idv.identitywaapproved_at,
            idv.identityphotosapproved_at,
            idv_3.idvthirdpartyapproved_br_at
          ) is not null then 'approved'
          when greatest(
            idv.identitywarejected_at,
            idv.identityphotosrejected_at,
            idv.prospectidentityverificationrejected_at,
            idv_3.idvthirdpartyrejected_br_at
          ) is not null then 'rejected'
          when (
            idv.application_id is not null
            or idv_3.application_id is not null
          )
          and greatest(
            idv.identitywadiscarded_at,
            idv.identityphotosdiscarded_at
          ) is not null
          and idv.used_policy_id ilike '%POLICY%' then 'discarded'
          when (
            idv.application_id is not null
            or idv_3.application_id is not null
          )
          and (
            (
              greatest(
                idv.identitywadiscarded_at,
                idv.identityphotosdiscarded_at
              ) is not null
              and idv.used_policy_id not ilike '%POLICY%'
            )
            or (
              greatest(
                idv.identitywadiscarded_at,
                idv.identityphotosdiscarded_at
              ) is null
            )
          ) then 'expired'
        end as idv_status
      ,COALESCE(idv.idv_provider, idv_3.idv_tp_provider) AS idv_provider
      ,COALESCE(idv.last_event_name_processed, idv_3.last_event_name_processed) AS idv_status_final_event
      ,case
        when client_loan_number.loan_number = 1 then 'PROSPECT'
        when client_loan_number.loan_number > 1 then 'CLIENT'
        else null
        end as client_type_ln
      ,client_loan_number.loan_number
      ,ls.calculation_date
      ,ls.is_fully_paid
      ,fpd.fully_paid_date
      ,fpd.n_previous_fully_paid_loans
      ,ls.min_payment
      ,ls.paid_installments
      ,ls.total_payment_applied
      ,ls.days_past_due
      ,ls.principal_overdue
      ,ls.total_principal_paid
      ,ls.interest_overdue
      ,ls.guarantee_overdue
      ,ls.unpaid_principal
      ,ls.unpaid_interest
      ,(ls.total_current_principal_condoned + ls.total_principal_overdue_condoned + ls.total_unpaid_principal_condoned) as principal_condoned
      ,(ls.total_current_interest_condoned + ls.total_interest_overdue_condoned + total_moratory_interest_condoned) as interest_condoned
      ,ls.total_guarantee_condoned as guarantee_condoned
      ,ls.refinancing_process_type
      ,cast(lp.interest_rate as double) as interest_rate
      ,lp.total_interest
    from silver.f_applications_br a
    left join silver.f_originations_bnpl_br orig on a.application_id = orig.application_id
    left join silver.f_originations_bnpn_br orig_bnpn on a.application_id = orig_bnpn.application_id
    left join silver.f_underwriting_fraud_stage_br udw on a.application_id = udw.application_id
    left join silver.d_ally_management_stores_allies_br ab on a.ally_slug=ab.ally_slug and a.store_slug = ab.store_slug -- allies
    left join gold.dm_loan_status_br ls on orig.loan_id = ls.loan_id
    left join silver.d_syc_clients_br sc on a.client_id = sc.client_id
    left join (select a.loan_id,
                      row_number() over(partition by a.client_id order by a.origination_date) as loan_number
               from silver.f_originations_bnpl_br a
               where a.loan_id is not null) client_loan_number on orig.loan_id = client_loan_number.loan_id
    left join gold.rmt_loan_fully_paid_date_br fpd on orig.loan_id = fpd.loan_id
    left join silver.f_loan_proposals_br lp on orig.loan_id = lp.loan_proposal_id
    left join silver.f_idv_stage_br idv on a.application_id = idv.application_id
    left join silver.f_idv_third_party_br idv_3 on a.application_id = idv_3.application_id
    left join silver.f_origination_events_br orig_ev on a.application_id = orig_ev.application_id
    left join gold.rmt_application_journey_stages_br app_js on a.application_id = app_js.application_id
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
        when days_past_due = 0 and first_payment_date >= calculation_date then 'Current'
        when first_payment_date < calculation_date then 'Before First Payment'
        end as dq_buckets
       , case when credit_score<150 then '1. <150'
        when credit_score<=260 then '2. 150-260'
        when credit_score <=400 then '3. 260-400'
        when credit_score <=500 then '4. 400-500'
        when credit_score <=600 then '5. 500-600'
        when credit_score <=800 then '6. 600-800'
        when credit_score>800 then '7. >800'
        end as score_range
     , case when credit_score<150 then '29% - 31%'
        when credit_score<=260 then '25% - 29%'
        when credit_score <=400 then '17% - 25%'
        when credit_score <=500 then '13% - 17%'
        when credit_score <=600 then '10% - 13%'
        when credit_score <=800 then '5% - 10%'
        when credit_score>800 then '0% - 5%'
        else '31+'
        end as Addi_PD_range
  from apps
)
, consolidated as (
    select distinct
      apps.application_id
      ,apps.loan_id
      ,apps.loan_originated
      ,apps.prospect_id
      ,cast(apps.general_date as timestamp) as application_date_time --Origination/Application date with time
      ,from_utc_timestamp(apps.general_date, 'America/Sao_Paulo') as application_date_time_local
      ,apps.general_date::date as d_vintage --Origination/Application day
      ,trunc(apps.general_date, 'week') as w_vintage --Origination/Application week
      ,date_format(apps.general_date, "yyyy-MM") as m_vintage --Origination/Application month
      ,date_format(apps.general_date, "yyyy-QQ") as q_vintage --Origination/Application quarter
      ,date_part('day',apps.general_date) as day_of_month
      ,apps.product
      ,apps.general_amount as amount --Requested amount
      ,apps.approved_amount
      ,apps.requested_amount
      ,apps.general_amount
      ,apps.preapproval_amount
      ,cast(apps.term as int) as term
      ,cast(apps.mdf as double) as mdf
      ,apps.ally_brand
      ,apps.ally_slug
      ,apps.ally_name
      ,apps.ally_vertical
      ,cast(coalesce(pdm.final_pd_score, apps.addi_pd) as double) as addi_pd_multiplied
      ,cast(coalesce(pdm.pd_no_multipliers, apps.addi_pd) as double) as addi_pd
      ,cast(pdm.final_pd_score as double) as final_pd_score
      ,pdm.pd_model_name
      ,cast(pdm.original_pd_score as double) as original_pd_score
      ,pdm.pd_multiplier
      ,cast(pdm.final_fpd_score as double) as final_fpd_score
      ,cast(pdm.pd_no_multipliers as double) as pd_no_multipliers
      ,cast(apps.bureau_pd as double) as bureau_pd
      ,apps.remaining_addicupo
      ,cast(apps.credit_score as bigint) as credit_score
      ,apps.credit_score_name
      ,score_range
      ,Addi_PD_range
      ,apps.journey_name
      ,apps.journey_stage_name
      ,apps.stage
      ,apps.journey_stages
      ,apps.stages
      ,apps.is_bnpn
      ,apps.credit_status_reason
      ,coalesce(pdm.credit_policy_name, apps.credit_policy_name) as credit_policy_name
      -- IDV Metrics 
      ,apps.bypass
      ,apps.possible_idv
      ,apps.idv_started
      ,apps.idv_approved
      ,case 
        when hia.first_idv_approval_date < apps.general_date then hia.idv_approved else 0 
        end as idv_prev_approved
      ,apps.idv_status
      ,apps.campaign_id
      ,apps.DP_payment
      ,substring(apps.campaign_id,14,3) as marketing_channel
      ,apps.client_type 
      ,apps.Discounts
      ,apps.perc_Discounts
      ,apps.client_type_ln --Client type deduced from loan number
      ,case
        when apps.application_date > d.min_preapproval_date then 'Preapproval'
        else 'Not Preapproval'
        end as preapproval_client
      ,case 
        when ( apps.application_date > app_pr.application_date
                and apps.requested_amount <= app_pr.preapproval_amount
                and app_pr.ally_preapproval is not null ) then 'ADDI Preapproval'
        else 'Not Preapproval'
        end as preapproval_application
      ,apps.imply_agg
      ,apps.application_channel
      ,apps.application_number
      , case when apps.application_date > app_pr.application_date and apps.requested_amount <= app_pr.preapproval_amount then apps.application_number -1 else apps.application_number end as app_num_adj
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
      --Performance data
      ,apps.calculation_date as current_status_calc_date
      ,floor(months_between(CURRENT_DATE(), apps.general_date::date)) AS mob
      ,apps.is_fully_paid
      ,apps.fully_paid_date
      ,apps.n_previous_fully_paid_loans
      ,apps.paid_installments
      ,apps.days_past_due
      ,apps.unpaid_principal
      ,apps.unpaid_interest
      ,apps.principal_condoned
      ,apps.interest_condoned
      ,apps.guarantee_condoned
      ,apps.total_principal_paid
      --Client metrics
      ,dm_cs.max_days_past_due
      ,dm_cs.installment_paid_in_delinquency_n
      ,dm_cs.installment_paid_in_delinquency_proportion
      ,dm_cs.n_active_days
      ,dm_cs.first_contact
      ,case when first_contact < apps.general_date then True else False end as has_contacted_before
      --Cancellation data
      ,case when dflc.cancellation_reason is not null then true else false end as is_cancelled
      ,cast(dflc.cancellation_date as date) as cancellation_date
      ,dflc.cancellation_reason
      --GROUPING
      ,g.dq_buckets
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
      --<Imply Variables>--
      , imp.background_check_br_in
      , imp.background_check_br_out
      , imp.device_information_in
      , imp.device_information_out
      , imp.fraud_check_br_in
      , imp.fraud_check_br_out
      --, imp.fraud_check_rc_br_in
      --, imp.fraud_check_rc_br_out
      , imp.loan_proposals_br_in
      , imp.loan_proposals_br_out
      , imp.preconditions_in
      , imp.preconditions_out
      , imp.underwriting_in
      , imp.underwriting_out
      , imp.loan_acceptance_in
      , imp.loan_acceptance_out
      , imp.identity_in
      , imp.identity_out
      , imp.additional_information_br_in
      , imp.additional_information_br_out
      , imp.banking_license_partner_br_in
      , imp.banking_license_partner_br_out
      , imp.basic_identity_br_in
      , imp.basic_identity_br_out
      , imp.bn_pn_payments_br_in
      , imp.bn_pn_payments_br_out
      , imp.cellphone_validation_br_in
      , imp.cellphone_validation_br_out
      , imp.down_payment_br_in
      , imp.down_payment_br_out
      , imp.personal_information_br_in
      , imp.personal_information_br_out
      , imp.preapproval_summary_br_in
      , imp.preapproval_summary_br_out
      , imp.privacy_policy_br_in
      , imp.privacy_policy_br_out
      , imp.psychometric_assessment_br_in
      , imp.psychometric_assessment_br_out
      , imp.subproduct_selection_br_in
      , imp.subproduct_selection_br_out
  ----<ECM variables>--
     --,k.risk_level ECM_risk_level
      , apps.bnpn_received_pay
      , case when rsc.client_id is not null then true else false end as backbook_seg_2
      , cast(apps.interest_rate as double) as interest_rate
      , cast(apps.total_interest as double) as total_interest
    from apps
    left join gold.rmt_application_preapproval_br app_pr
      on apps.prospect_id = app_pr.prospect_id 
      and apps.application_date between app_pr.application_date and app_pr.preapproval_expiration_date 
      and rn = 1
    left join gold.rmt_first_preapproval_date_br d on apps.prospect_id = d.client_id
    left join grouping_buckets g on apps.application_id = g.application_id
    left join gold.dm_customer_segmentation dm_cs on apps.prospect_id = dm_cs.client_id and dm_cs.country = 'BR'
    left join gold.rmt_historic_idv_approved_br hia on apps.prospect_id = hia.client_id 
    left join gold.rmt_metrics_br met on apps.loan_id = met.loan_id
    left join gold.rmt_imply_risk_br imp on from_utc_timestamp(apps.application_date, 'America/Sao_Paulo')::date = imp.day
                                     and apps.ally_slug = imp.ally_name 
                                     and apps.journey_name = imp.journey_name
                                     and apps.prospect_id = imp.client_id
    left join silver.f_loan_cancellation_br dflc on apps.loan_id = dflc.loan_id
    left join gold.rmt_pd_models_co pdm on apps.application_id = pdm.application_id
    left join risk.seg2_cut_br rsc on apps.prospect_id = rsc.client_id --backbook surgery
)
select * from consolidated