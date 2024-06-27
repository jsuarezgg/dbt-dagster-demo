{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

with apps as (
    select 
      apps.application_id
      ,COALESCE(orig.loan_id, refi_orig.loan_id) AS loan_id
      ,apps.client_id as prospect_id
      ,apps.store_user_id
      ,COALESCE(orig.origination_date, refi_orig.origination_date) AS origination_date
      ,apps.application_date
      ,apps.campaign_id
      ,apps.application_date as general_date
      ,COALESCE(orig.approved_amount,refi_orig.approved_amount) AS approved_amount
      ,sc.remaining_addicupo
      ,apps.requested_amount
      ,coalesce(apps.requested_amount_without_discount, apps.requested_amount) as amount_before_discount
      ,case 
        when (COALESCE(orig.approved_amount, refi_orig.approved_amount) is null or COALESCE(orig.approved_amount,refi_orig.approved_amount) = 0) then apps.requested_amount
        else COALESCE(orig.approved_amount, refi_orig.approved_amount)
        end as general_amount
      ,case 
        when (orig.application_id is not null OR refi_orig.application_id IS NOT NULL) then true
        else false 
        end as loan_originated
      ,lp.term
      ,apps.ally_slug
      ,al.ally_name
      ,als.store_slug as store_name
      ,al.brand_name as ally_brand
      ,al.vertical_name as ally_vertical
      ,al.ally_state as ally_state
      ,case when al.ally_state NOT IN ('CREATED', 'PRE_ACTIVE', 'ACTIVE', 'COLD', 'CHURNED') then true else false end as ally_is_terminated
      ,ar.region_name as ally_region
      ,udw.credit_policy_name
      ,udw.probability_default_addi as addi_pd
      ,udw.probability_default_bureau as bureau_pd
      ,udw.credit_score
      ,udw.credit_score_name
      ,addiv2.evaluation_type
      ,apps.journey_name
      ,orig_ev.journey_stage_name
      ,orig_ev.last_event_name_processed as stage
      ,orig_ev.event_type as final_event_type
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
            orig.application_id is not null or refi_orig.application_id IS NOT NULL
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
        when apps.channel like '%REFINANCE%' then 'REFINANCE'
        when apps.channel like '%ADDI_MARKETPLACE%' then 'ADDI_MARKETPLACE'
        WHEN apps.channel IS NULL THEN NULL
        ELSE '_PENDING_MANUAL_MAPPING_'
      end as application_channel
      ,bl.processed_product as product
      ,bl.original_product as application_product
      ,bl.synthetic_product_category
      ,bl.synthetic_product_subcategory
      ,apps.preapproval_amount
      ,row_number() over(partition by apps.client_id order by apps.application_date) as application_number
      -- Custom calculations from risk - Context on whether or not we change the partition by: https://addico.slack.com/archives/C01KS5QMGQL/p1716998693409679?thread_ts=1716998115.938689&cid=C01KS5QMGQL
      ,row_number() over (partition by funnel_bl.application_process_id order by apps.application_date desc) as imply_agg
      ,row_number() over (partition by funnel_bl.application_process_id,
                                       bl.synthetic_product_category order by apps.application_date desc) as app_process_synthetic_product_order
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
      ,CASE WHEN COALESCE(orig.term, refi_orig.term) > 3 AND apps.product = 'PAGO_CO' THEN True ELSE False END AS is_flex
      ,CASE WHEN COALESCE(orig.custom_is_santander_originated,FALSE) IS NOT TRUE THEN lp.total_interest ELSE 0 END AS total_interest
      ,COALESCE(orig.guarantee_rate, refi_orig.guarantee_rate) AS guarantee_rate
      ,CASE WHEN orig.guarantee_provider IS NOT NULL THEN orig.guarantee_provider
             WHEN orig.guarantee_provider IS NULL
                AND COALESCE(orig.guarantee_rate, refi_orig.guarantee_rate) > 0.0 THEN 'FGA'
             ELSE NULL END AS guarantee_provider_with_default
      ,funnel_dm.ocurred_on_date as ocurred_on_date_funnel
      ,funnel_bl.application_process_id
      ,CASE WHEN mta.application_id IS NOT NULL THEN True ELSE False END is_attributable_transaction
      ,ls.unpaid_collection_fees
      ,ls.total_collection_fees_paid
      ,ls.total_collection_fees_condoned
    from {{ ref('f_applications_co') }} apps
    LEFT JOIN {{ ref('bl_application_id_to_application_process_id_co') }} AS funnel_bl ON funnel_bl.application_id = apps.application_id
    LEFT JOIN {{ ref('dm_application_process_funnel_co') }}            AS funnel_dm ON funnel_dm.application_process_id = funnel_bl.application_process_id
    left join {{ ref('f_originations_bnpl_co') }} orig on apps.application_id = orig.application_id
    left join {{ ref('f_refinance_loans_co') }} AS refi_orig ON apps.application_id = refi_orig.application_id
    left join {{ ref('f_underwriting_fraud_stage_co') }} udw on apps.application_id = udw.application_id
    left join {{ ref('d_syc_clients_co') }} sc on apps.client_id = sc.client_id
    left join {{ ref('d_ally_management_allies_co') }} al on apps.ally_slug = al.ally_slug
    left join {{ ref('d_ally_management_stores_allies_co') }} als on apps.ally_slug = als.ally_slug and apps.store_slug = als.store_slug
    left join {{ ref('d_ally_management_cities_regions_co')}} ar on als.store_city_code = ar.city_code 
    left join (select a.loan_id, 
                      row_number() over(partition by a.client_id order by a.origination_date) as loan_number
               from {{ ref('f_originations_bnpl_co') }} a
               where a.loan_id is not null) client_loan_number on orig.loan_id = client_loan_number.loan_id
    left join {{ ref('bl_application_product_co') }} bl on apps.application_id = bl.application_id
    left join {{ ref('dm_loan_status_co') }} ls on COALESCE(orig.loan_id,refi_orig.loan_id) = ls.loan_id
    left join {{ source('gold', 'rmt_loan_fully_paid_date_co') }} fpd on COALESCE(orig.loan_id,refi_orig.loan_id) = fpd.loan_id and COALESCE(orig.client_id,refi_orig.client_id) = fpd.client_id
    left join {{ ref('f_loan_proposals_co') }} lp on COALESCE(orig.loan_id,refi_orig.loan_id) = lp.loan_proposal_id
    left join {{ ref('f_idv_stage_co') }} idv on apps.application_id = idv.application_id
    left join {{ ref('f_identity_photos_third_party_co') }} idv_3 on apps.application_id = idv_3.application_id
    left join {{ ref('f_origination_events_co') }} orig_ev on apps.application_id = orig_ev.application_id
    left join {{ ref('rmt_application_journey_stages_co') }} app_js on apps.application_id = app_js.application_id
    left join {{ ref('dm_addishop_paying_allies_co') }} la on orig.ally_slug = la.ally_slug
        and orig.origination_date >= to_timestamp(la.start_date)
        and orig.origination_date <= to_timestamp(la.end_date)
    left join {{ ref('f_applications_addi_v2_co')}} addiv2 on apps.application_id = addiv2.application_id
    left join {{ ref('f_marketplace_transaction_attributable_co') }} mta on apps.application_id = mta.application_id
)

select *
from apps;
