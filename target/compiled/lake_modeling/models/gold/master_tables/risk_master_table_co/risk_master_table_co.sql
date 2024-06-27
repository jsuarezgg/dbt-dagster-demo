

with apps as (
    select 
      apps.application_id
      ,orig.loan_id
      ,apps.client_id as prospect_id
      ,apps.store_user_id
      ,orig.origination_date
      ,apps.application_date
      ,apps.campaign_id
      ,coalesce(orig.origination_date, apps.application_date) as general_date
      ,orig.approved_amount
      ,sc.remaining_addicupo
      ,apps.requested_amount
      ,coalesce(apps.requested_amount_without_discount, apps.requested_amount) as amount_before_discount
      ,case 
        when (orig.approved_amount is null or orig.approved_amount = 0) then apps.requested_amount
        else orig.approved_amount 
        end as general_amount
      ,case 
        when orig.application_id is not null then true
        else false 
        end as loan_originated
      ,lp.term
      ,apps.ally_slug
      ,al.ally_name
      ,al.store_slug as store_name
      ,al.ally_brand:['name']['value'] as ally_brand
      ,al.ally_vertical:['name']['value'] as ally_vertical
      ,ar.region_name as ally_region
      ,udw.credit_policy_name
      ,udw.probability_default_addi as addi_pd
      ,udw.probability_default_bureau as bureau_pd
      ,udw.credit_score
      ,udw.credit_score_name
      ,apps.journey_name
      ,orig_ev.journey_stage_name
      ,orig_ev.last_event_name_processed as stage
      ,app_js.journey_stages
      ,app_js.stages
      ,udw.credit_status_reason
      ,coalesce(udw.learning_population, false) as learning_population
      ,coalesce(udw.lbl, false) as low_balance_loan
      ,udw.group_name as financial_experience
      ,case 
        when apps.client_type is not null then (case when apps.client_type = 'LEAD' then 'PROSPECT' else apps.client_type end) 
        when apps.custom_is_returning_client_legacy is not true then 'PROSPECT' 
        else 'CLIENT' 
        end as client_type
      -- IDV Stats and metrics
      ,CASE
        WHEN (
          idv.prospectidentityverificationstarted_at is null
          and idv.identityphotosstarted_at is null
          and idv.identitywastarted_at is null
          and idv_3.identityphotosthirdpartystarted_co_at is null
        )
        AND (
            orig.application_id is not null
          ) then 1
        else 0
      end as bypass
      ,CASE
        WHEN (
          idv.prospectidentityverificationstarted_at is not null
          or idv.identityphotosstarted_at is not null
          or idv.identitywastarted_at is not null
          or idv_3.identityphotosthirdpartystarted_co_at is not null
        )
        or (
            idv.application_id is not null
            or idv_3.application_id is not null
          ) then 1
        else 0
      end as possible_idv
      ,CASE
        WHEN (
          idv.prospectidentityverificationstarted_at is not null
          or idv.identityphotosstarted_at is not null
          or idv.identitywastarted_at is not null
          or idv_3.identityphotosthirdpartystarted_co_at is not null
        ) then 1
        else 0
      end as idv_started
      ,CASE
        WHEN (
          idv.prospectidentityverificationinputinformationcompleted_at is not null
          or idv.identityphotosapproved_at is not null
          or idv.identitywaapproved_at is not null
          or idv_3.identityphotosthirdpartyapproved_co_at is not null
        ) then 1
        else 0
      end as idv_approved
      ,CASE
      WHEN greatest(
        idv.prospectidentityverificationinputinformationcompleted_at,
        idv.identitywaapproved_at,
        idv.identityphotosapproved_at,
        idv_3.identityphotosthirdpartyapproved_co_at
      ) is not null then 'approved'
      WHEN greatest(
        idv.prospectidentityverificationrejected_at,
        idv.identitywarejected_at,
        idv.identityphotosrejected_at,
        idv_3.identityphotosthirdpartyrejected_co_at
      ) is not null then 'rejected'
      WHEN (
        (
          idv.application_id is not null
          or idv_3.application_id is not null
        )
      )
      and (
        greatest(
          idv.identitywadiscarded_at,
          idv.identityphotosdiscarded_at,
          idv_3.identityphotosthirdpartydiscarded_co_at
        ) is not null
        and (
          idv.used_policy_id ilike '%POLICY%'
          or idv_3.application_id is not null
        )
      )
      or (
        greatest(
          idv.prospectidentityverificationdiscardedbycreditrisk_at,
          idv.identityphotosdiscardedbyrisk_at,
          idv.identitywadiscardedbyrisk_at,
          idv_3.identityphotosthirdpartydiscarded_co_at,
          idv_3.identityphotosthirdpartydiscardedbyrisk_co_at
        ) is not null
      ) then 'discarded'
      WHEN
        (
          (
            idv.application_id is not null
            or idv_3.application_id is not null
          )
        )
        and (
          (
            greatest(
              idv.identitywadiscarded_at,
              idv.identityphotosdiscarded_at,
              idv_3.identityphotosthirdpartydiscarded_co_at
            ) is not null
            and (
              idv.used_policy_id not ilike '%POLICY%'
              or idv_3.application_id is not null
            )
          )
          or (
            greatest(
              idv.identitywadiscarded_at,
              idv.identityphotosdiscarded_at,
              idv_3.identityphotosthirdpartydiscarded_co_at
            ) is null
            )) then 'expired'
        WHEN (
          idv.application_id is not null
          or idv_3.application_id is not null
        )
        and greatest(
          idv.identitywadiscarded_at,
          idv.identityphotosdiscarded_at,
          idv_3.identityphotosthirdpartydiscarded_co_at,
          idv.prospectidentityverificationinputinformationcompleted_at,
          idv.identitywaapproved_at,
          idv.identityphotosapproved_at,
          idv_3.identityphotosthirdpartyapproved_co_at,
          idv.prospectidentityverificationrejected_at,
          idv.identitywarejected_at,
          idv.identityphotosrejected_at,
          idv_3.identityphotosthirdpartyrejected_co_at
        ) is null then 'decline'
        ELSE null
      END as idv_status
      ,COALESCE(idv.idv_provider, idv_3.idv_provider) AS idv_provider
      ,coalesce(idv.last_event_name_processed, idv_3.last_event_name_processed) as idv_status_final_event
      ,cast(lp.interest_rate as double) as interest_rate
      ,from_utc_timestamp(ls.first_payment_date, 'America/Bogota') as first_payment_date
      ,case 
        when apps.channel like '%PAY%LINK%' then 'PAY_LINK'
        when apps.channel like '%E%COMMERCE%' then 'E_COMMERCE'
        when apps.channel like '%PRE%APPROVAL%' then 'PREAPPROVAL'
        when apps.channel like '%IN%STORE%' then 'IN_STORE'
        end as application_channel
      ,ap.product
      ,apps.preapproval_amount
      ,row_number() over(partition by apps.client_id order by apps.application_date) as application_number
      ,row_number() over (partition by apps.client_id, apps.journey_name, apps.ally_slug, apps.application_date::date order by apps.application_date desc) as imply_agg
      ,client_loan_number.loan_number as loan_number
      ,ls.calculation_date
      ,ls.is_fully_paid
      ,fpd.fully_paid_date
      ,fpd.n_previous_fully_paid_loans
      ,ls.min_payment
      ,ls.paid_installments
      ,ls.total_payment_applied
      ,ls.total_principal_paid
      ,ls.days_past_due
      ,ls.principal_overdue
      ,ls.interest_overdue
      ,ls.guarantee_overdue
      ,ls.unpaid_principal
      ,ls.unpaid_interest
      ,ls.unpaid_guarantee
      ,(ls.total_current_principal_condoned + ls.total_principal_overdue_condoned + ls.total_unpaid_principal_condoned) as principal_condoned
      ,(ls.total_current_interest_condoned + ls.total_interest_overdue_condoned + total_moratory_interest_condoned) as interest_condoned
      ,ls.total_guarantee_condoned as guarantee_condoned
      ,ls.refinancing_process_type
      ,ls.cancellation_reason
      ,lp.ally_mdf as mdf
      ,ls.total_fga_rate as fga
      ,ls.fga_client_rate as fga_effective
      ,CASE WHEN ls.state IN ('CANCELLED_BY_ALLY','CANCELLED_BY_FRAUD') THEN True ELSE False END AS bad_loan
      ,COALESCE(la.lead_gen_fee,0) AS lead_gen_fee
      ,CASE WHEN orig.term > 3 AND apps.product = 'PAGO_CO' THEN True ELSE False END AS is_flex
      ,CASE WHEN apps.product = 'PAGO_CO' THEN lp.total_interest ELSE 0 END AS total_interest
      ,orig.guarantee_rate
    from silver.f_applications_co apps
    left join silver.f_originations_bnpl_co orig on apps.application_id = orig.application_id
    left join silver.f_underwriting_fraud_stage_co udw on apps.application_id = udw.application_id
    left join silver.d_syc_clients_co sc on apps.client_id = sc.client_id
    left join silver.d_ally_management_stores_allies_co al on apps.ally_slug = al.ally_slug and apps.store_slug = al.store_slug
    left join silver.d_ally_management_cities_regions_co ar on al.store_city_code = ar.city_code 
    left join (select a.loan_id, 
                      row_number() over(partition by a.client_id order by a.origination_date) as loan_number
               from silver.f_originations_bnpl_co a
               where a.loan_id is not null) client_loan_number on orig.loan_id = client_loan_number.loan_id
    left join gold.rmt_application_product_co ap on apps.application_id = ap.application_id
    left join gold.dm_loan_status_co ls on orig.loan_id = ls.loan_id
    left join gold.rmt_loan_fully_paid_date_co fpd on orig.loan_id = fpd.loan_id
    left join silver.f_loan_proposals_co lp on orig.loan_id = lp.loan_proposal_id
    left join silver.f_idv_stage_co idv on apps.application_id = idv.application_id
    left join silver.f_identity_photos_third_party_co idv_3 on apps.application_id = idv_3.application_id
    left join silver.f_origination_events_co orig_ev on apps.application_id = orig_ev.application_id
    left join gold.rmt_application_journey_stages_co app_js on apps.application_id = app_js.application_id
    left join gold.dm_addishop_paying_allies_co la on orig.ally_slug = la.ally_slug
        and orig.origination_date >= to_timestamp(la.start_date)
        and orig.origination_date <= to_timestamp(la.end_date)

)
 --Get PII Information - Age Range
, pii_information as (
select application_id
          ,max(personId_ageRange) as prospect_age_range
          ,case when max(personId_ageRange) = "18-21" then 19.5
          when max(personId_ageRange) = "18-25" then 21.5
          when max(personId_ageRange) = "22-28" then 25.0
          when max(personId_ageRange) = "25-30" then 27.5
          when max(personId_ageRange) = "29-35" then 32.0
          when max(personId_ageRange) = "31-35" then 33.0
          when max(personId_ageRange) = "36-40" then 38.0
          when max(personId_ageRange) = "36-45" then 40.5
          when max(personId_ageRange) = "41-45" then 43.0
          when max(personId_ageRange) = "46-50" then 48.0
          when max(personId_ageRange) = "46-55" then 50.5
          when max(personId_ageRange) = "51-55" then 53.0
          when max(personId_ageRange) = "56-60" then 58.0
          when max(personId_ageRange) = "56-65" then 60.5
          when max(personId_ageRange) = "61-65" then 63.0
          when max(personId_ageRange) = "66-70" then 68.0
          when max(personId_ageRange) = "71-75" then 73.0
          when max(personId_ageRange) = "Mas 75" then 78.0 end as prospect_age_avg
    from silver.f_kyc_bureau_personal_info_co
    group by application_id
)
-- Get PII Information

, kyc_addresses_last as (
    select application_id,
           ocurred_on,
           bureau_address_city as id_exp_city,
           row_number() over (partition by application_id order by ocurred_on desc) as rn
    from silver.f_kyc_bureau_contact_info_addresses_co_logs
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
    left join gold.expedition_city_homologation_co ech on ka.id_exp_city = ech.id_exp_city
    where ka.rn = 1
)
-- Get PII Information
, kyc_id_region_v2 as (
    select
      aci.application_id
      --,a.id_exp_city as old_exp_city
      ,ech.city_name as id_exp_city
      ,ech.region_name as id_exp_region
      ,row_number() over(partition by aci.application_id order by aci.application_id) as rn
    from gold.rmt_application_city_information_co aci
    left join gold.expedition_city_homologation_co ech on aci.id_expedition = ech.id_exp_city
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
        when days_past_due = 0 and first_payment_date >= calculation_date then 'Current'
        when first_payment_date < calculation_date then 'Before First Payment'
        end as dq_buckets
  from apps
) ,

apps_no_preapp as ( select application_id,
                           count(application_id) OVER (PARTITION BY client_id ORDER BY application_date::date desc ROWS BETWEEN 30 PRECEDING
        AND CURRENT ROW ) as apps_no_preapp
        from silver.f_applications_co 
        where channel NOT like '%PRE%APPROVAL%'
        and product = "PAGO_CO"
        and custom_is_returning_client_legacy is not true
),
clients_blacklisted AS (
    SELECT
        ppd.client_id,
        CASE WHEN bld.id_number IS NOT NULL
                OR ble.email IS NOT NULL
                OR blp.cell_phone_number IS NOT NULL
                OR r.client_id IS NOT NULL THEN True ELSE False END AS client_blacklisted
    FROM cur.pii_clients ppd
    LEFT JOIN cur.kyc_blacklisted_documents bld
        ON ppd.id_number = bld.id_number
    LEFT JOIN cur.kyc_blacklisted_emails ble
        ON ppd.last_application_email = ble.email
    LEFT JOIN cur.kyc_phone_blacklist blp
        ON ppd.last_application_cellphone = blp.cell_phone_number
    LEFT JOIN risk.comms_exclusion r
        ON ppd.client_id = r.client_id
)

---##<CONSOLIDATE EVERYTHING>##---
, consolidated as (
    select distinct
      apps.application_id
      ,apps.loan_id
      ,apps.loan_originated
      ,apps.prospect_id
      ,apps.store_user_id
      ,cast(apps.general_date as timestamp) as application_date_time --Origination/Application date with time
      ,from_utc_timestamp(apps.general_date, 'America/Bogota') as application_date_time_local
      ,apps.general_date::date as d_vintage --Origination/Application day
      ,trunc(apps.general_date, 'week') as w_vintage --Origination/Application week
      ,date_format(apps.general_date, "yyyy-MM") as m_vintage --Origination/Application month
      ,date_format(apps.general_date, "yyyy-QQ") as q_vintage --Origination/Application quarter
      ,apps.product
      ,apps.general_amount as amount --Approved/Requested amount
      ,apps.approved_amount
      ,apps.remaining_addicupo
      ,apps.requested_amount
      ,apps.amount_before_discount
      ,apps.preapproval_amount
      ,cast(apps.term as int) as term
      ,cast(apps.interest_rate as double) as interest_rate
      ,cast(apps.mdf as double) as mdf
      ,apps.fga
      ,apps.fga_effective
      ,apps.ally_slug
      ,apps.ally_name
      ,apps.store_name
      ,apps.ally_brand
      ,apps.ally_vertical
      ,case when ta.ally_slug is not null then true else false end as ally_is_terminated --Need to add
      ,ta.last_day as ally_terminated_date --last_origination_date
      ,apps.ally_region      
      ,kyc_id.id_exp_city as id_exp_city
      ,kyc_id.id_exp_region as id_exp_region
      ,cast(coalesce(pdm.final_pd_score, apps.addi_pd) as double) as addi_pd_multiplied
      ,cast(coalesce(pdm.pd_no_multipliers, apps.addi_pd) as double) as addi_pd
      ,cast(pdm.final_pd_score as double) as final_pd_score
      ,pdm.pd_model_name
      ,cast(pdm.original_pd_score as double) as original_pd_score
      ,pdm.pd_multiplier
      ,cast(pdm.final_fpd_score as double) as final_fpd_score
      ,cast(pdm.pd_no_multipliers as double) as pd_no_multipliers
      ,cast(apps.bureau_pd as double) as bureau_pd
      ,cast(apps.credit_score as bigint) as credit_score
      ,apps.credit_score_name      
      ,apps.journey_name
      ,apps.journey_stage_name
      ,apps.stage
      ,apps.journey_stages
      ,apps.stages
      ,apps.credit_status_reason
      ,coalesce(pdm.credit_policy_name, apps.credit_policy_name) as credit_policy_name
      ,apps.low_balance_loan
      ,apps.campaign_id
      ,substring(apps.campaign_id,14,3) as marketing_channel
      ,pii.prospect_age_range
      ,pii.prospect_age_avg
      
      ,apps.learning_population
      ,case when apps.learning_population is true 
                  or pdm2.application_id is not null then true else false end as ia_loan
      ,apps.client_type --Client type extracted from database
      
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
      ,apps.application_number
      ,app_no_pr.apps_no_preapp
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
      --Performance data
      ,apps.calculation_date as current_status_calc_date
      ,floor(months_between(CURRENT_DATE(), apps.general_date::date)) AS mob
      ,apps.is_fully_paid
      ,apps.fully_paid_date
      ,apps.n_previous_fully_paid_loans
      ,case when apps.cancellation_reason is not null then true else false end as is_cancelled
      ,cast(dflc.cancellation_date as date) as cancellation_date
      ,dflc.cancellation_reason
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
      --<Additional Variables>--
      , case when lo.sale_date is not null then lo.loan_ownership else "ADDI" end as funding_partner_ownership
      , lo.sale_date::date as funding_partner_sale_date
      --<Imply Variables>--
      , imp.background_check_co_in
      , imp.background_check_co_out
      , imp.device_information_in
      , imp.device_information_out
      , imp.fraud_check_co_in
      , imp.fraud_check_co_out
      --, imp.fraud_check_rc_co_in
      --, imp.fraud_check_rc_co_out
      , imp.loan_proposals_co_in
      , imp.loan_proposals_co_out
      --, imp.loan_proposals_santander_co_in
      --, imp.loan_proposals_santander_co_out
      , imp.preconditions_in
      , imp.preconditions_out
      , imp.underwriting_in
      , imp.underwriting_out
      , imp.loan_acceptance_in
      , imp.loan_acceptance_out
      , imp.identity_in
      , imp.identity_out
      , imp.additional_information_co_in
      , imp.additional_information_co_out
      , imp.basic_identity_co_in
      , imp.basic_identity_co_out
      , imp.cellphone_validation_co_in
      , imp.cellphone_validation_co_out
      , imp.personal_information_co_in
      , imp.personal_information_co_out
      , imp.preapproval_summary_co_in
      , imp.preapproval_summary_co_out
      , imp.privacy_policy_stage_in
      , imp.privacy_accepted_co_out
      , imp.privacy_expiration_date_co_out
      , imp.privacy_first_name_co_out
      , imp.privacy_policy_co_in
      , imp.privacy_policy_co_out
      , imp.privacy_policy_v2_co_in
      , imp.privacy_policy_v2_co_out
      , imp.psychometric_assessment_co_in
      , imp.psychometric_assessment_co_out
      , imp.work_information_co_in
      , imp.work_information_co_out
      , CASE WHEN mta.application_id IS NOT NULL THEN True ELSE False END is_attributable_transaction
    from apps
    left join gold.rmt_application_preapproval_co app_pr
      on apps.prospect_id = app_pr.prospect_id 
      and apps.application_date between app_pr.application_date and app_pr.preapproval_expiration_date 
      and rn = 1
    left join gold.rmt_first_preapproval_date_co first_pr on apps.prospect_id = first_pr.client_id
    left join gold.rmt_metrics_co met on apps.loan_id = met.loan_id
    left join grouping_buckets gb on apps.application_id = gb.application_id
    left join gold.dm_customer_segmentation dm_cs on apps.prospect_id = dm_cs.client_id and dm_cs.country = 'CO'
    left join pii_information pii on apps.application_id = pii.application_id
    left join apps_no_preapp app_no_pr on apps.application_id = app_no_pr.application_id
    --<Additional Variables>--
    left join gold.rmt_historic_idv_approved_co hia on apps.prospect_id = hia.client_id
    left join gold.rmt_terminated_allies_co ta on apps.ally_slug = ta.ally_slug
    left join kyc_id_region_unified kyc_id on apps.application_id = kyc_id.application_id
    left join gold.loan_ownership_co lo on apps.loan_id = lo.loan_id
    left join gold.rmt_pd_models_co pdm on apps.application_id = pdm.application_id
    left join gold.rmt_pd_models_co pdm2 on apps.application_id=pdm2.application_id and pdm2.final_pd_score < pdm2.original_pd_score --IA loans have a lower final PD
    left join gold.rmt_imply_risk_co imp on from_utc_timestamp(apps.application_date, 'America/Bogota')::date = imp.day 
                                     and apps.ally_slug = imp.ally_name 
                                     and apps.journey_name = imp.journey_name
                                     and apps.prospect_id = imp.client_id
    left join silver.f_loan_cancellation_co dflc on apps.loan_id = dflc.loan_id
    left join clients_blacklisted cbl on apps.prospect_id = cbl.client_id
    left join silver.f_marketplace_transaction_attributable_co mta on apps.application_id = mta.application_id
)

select * from consolidated