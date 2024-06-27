with gmv as (
select t.client_id,
      'CO' as country_code,
       (from_utc_timestamp(t.origination_date,'America/Bogota'))::date as period,
       date_trunc('week',(from_utc_timestamp(t.origination_date,'America/Bogota'))::date) as week,
       date_format((from_utc_timestamp(t.origination_date,'America/Bogota'))::date, "yyyy-MM") as month,
       date_format((from_utc_timestamp(t.origination_date,'America/Bogota'))::date, "yyyy-QQ") as quarter, 
       SUM(case when app.requested_amount_without_discount >0 then app.requested_amount_without_discount
                else app.requested_amount 
           end) as gmv,
       SUM(case when requested_amount_without_discount > 0 and (product = 'PAGO_CO' 
                or credit_policy_name in ('addipago_0aprfga_policy', 'addipago_0fga_policy', 'addipago_claro_policy', 'addipago_mario_h_policy', 'addipago_no_history_policy', 'addipago_policy', 'addipago_policy_amoblando', 'adelante_policy_pago', 'closing_policy_pago', 'finalization_policy_pago', 'rc_0aprfga', 'rc_addipago_policy_amoblando', 'rc_closing_policy', 'rc_finalization_policy', 'rc_pago_0aprfga', 'rc_pago_0fga', 'rc_pago_claro', 'rc_pago_mario_h', 'rc_pago_standard', 'rc_rejection_policy')) then requested_amount_without_discount
                when (product = 'PAGO_CO' 
                or credit_policy_name in ('addipago_0aprfga_policy', 'addipago_0fga_policy', 'addipago_claro_policy', 'addipago_mario_h_policy', 'addipago_no_history_policy', 'addipago_policy', 'addipago_policy_amoblando', 'adelante_policy_pago', 'closing_policy_pago', 'finalization_policy_pago', 'rc_0aprfga', 'rc_addipago_policy_amoblando', 'rc_closing_policy', 'rc_finalization_policy', 'rc_pago_0aprfga', 'rc_pago_0fga', 'rc_pago_claro', 'rc_pago_mario_h', 'rc_pago_standard', 'rc_rejection_policy')) then requested_amount 
           end) as gmv_pago_co,
       SUM(case when requested_amount_without_discount > 0 and product = 'FINANCIA_CO' then requested_amount_without_discount
                when product = 'FINANCIA_CO' then requested_amount 
           end) as gmv_financia_co,

       SUM(1) as transactions,
       SUM(case when (client_type = 'CLIENT') AND (product = 'PAGO_CO'  
                or credit_policy_name in ('addipago_0aprfga_policy', 'addipago_0fga_policy', 'addipago_claro_policy', 'addipago_mario_h_policy', 'addipago_no_history_policy', 'addipago_policy', 'addipago_policy_amoblando', 'adelante_policy_pago', 'closing_policy_pago', 'finalization_policy_pago', 'rc_0aprfga', 'rc_addipago_policy_amoblando', 'rc_closing_policy', 'rc_finalization_policy', 'rc_pago_0aprfga', 'rc_pago_0fga', 'rc_pago_claro', 'rc_pago_mario_h', 'rc_pago_standard', 'rc_rejection_policy')) then 1
           end) as transactions_pago_co_rc,
       SUM(case WHEN (client_type != 'CLIENT' OR client_type IS NULL) AND (product = 'PAGO_CO'
                or credit_policy_name in ('addipago_0aprfga_policy', 'addipago_0fga_policy', 'addipago_claro_policy', 'addipago_mario_h_policy', 'addipago_no_history_policy', 'addipago_policy', 'addipago_policy_amoblando', 'adelante_policy_pago', 'closing_policy_pago', 'finalization_policy_pago', 'rc_0aprfga', 'rc_addipago_policy_amoblando', 'rc_closing_policy', 'rc_finalization_policy', 'rc_pago_0aprfga', 'rc_pago_0fga', 'rc_pago_claro', 'rc_pago_mario_h', 'rc_pago_standard', 'rc_rejection_policy')) then 1
           end) as transactions_pago_co_prospect,
       SUM(case when product = 'FINANCIA_CO' AND (client_type = 'CLIENT') then 1 
           end) as transactions_financia_co_rc,
       SUM(case when product = 'FINANCIA_CO' AND (client_type != 'CLIENT' OR client_type IS NULL) then 1 
           end) as transactions_financia_co_prospect,

       SUM(case when (product = 'PAGO_CO' 
                or credit_policy_name in ('addipago_0aprfga_policy', 'addipago_0fga_policy', 'addipago_claro_policy', 'addipago_mario_h_policy', 'addipago_no_history_policy', 'addipago_policy', 'addipago_policy_amoblando', 'adelante_policy_pago', 'closing_policy_pago', 'finalization_policy_pago', 'rc_0aprfga', 'rc_addipago_policy_amoblando', 'rc_closing_policy', 'rc_finalization_policy', 'rc_pago_0aprfga', 'rc_pago_0fga', 'rc_pago_claro', 'rc_pago_mario_h', 'rc_pago_standard', 'rc_rejection_policy')) and client_type = 'PROSPECT' then 1 
           end) as originations_pago_co,
       SUM(case when product = 'FINANCIA_CO' and client_type = 'PROSPECT' then 1 
           end) as originations_financia_co,
       SUM((case when app.requested_amount_without_discount >0 then app.requested_amount_without_discount
                      else app.requested_amount 
            end)*l.ally_mdf
          ) as dmf_nominator,
       SUM(case when app.requested_amount_without_discount > 0 then app.requested_amount_without_discount 
                else app.requested_amount 
           end) as dmf_denominator,   --*100 as weigthed_DMF
       SUM((case when requested_amount_without_discount > 0 and (product = 'PAGO_CO' 
                or credit_policy_name in ('addipago_0aprfga_policy', 'addipago_0fga_policy', 'addipago_claro_policy', 'addipago_mario_h_policy', 'addipago_no_history_policy', 'addipago_policy', 'addipago_policy_amoblando', 'adelante_policy_pago', 'closing_policy_pago', 'finalization_policy_pago', 'rc_0aprfga', 'rc_addipago_policy_amoblando', 'rc_closing_policy', 'rc_finalization_policy', 'rc_pago_0aprfga', 'rc_pago_0fga', 'rc_pago_claro', 'rc_pago_mario_h', 'rc_pago_standard', 'rc_rejection_policy')) then requested_amount_without_discount
                when (product = 'PAGO_CO' 
                or credit_policy_name in ('addipago_0aprfga_policy', 'addipago_0fga_policy', 'addipago_claro_policy', 'addipago_mario_h_policy', 'addipago_no_history_policy', 'addipago_policy', 'addipago_policy_amoblando', 'adelante_policy_pago', 'closing_policy_pago', 'finalization_policy_pago', 'rc_0aprfga', 'rc_addipago_policy_amoblando', 'rc_closing_policy', 'rc_finalization_policy', 'rc_pago_0aprfga', 'rc_pago_0fga', 'rc_pago_claro', 'rc_pago_mario_h', 'rc_pago_standard', 'rc_rejection_policy')) then requested_amount 
           end)*l.ally_mdf
         ) as mdf_nominator_pago_co,
       SUM((case when (app.requested_amount_without_discount >0 or app.requested_amount_without_discount is distinct from '') and (custom_is_santander_branched is not true or (custom_is_santander_branched is true and (guarantee_rate <> 0 or pd_calculation_method = 'Addi PD'))) then app.requested_amount_without_discount
                 when custom_is_santander_branched is not true or (custom_is_santander_branched is true and (guarantee_rate <> 0 or pd_calculation_method = 'Addi PD')) then app.requested_amount 
            end)*l.ally_mdf 
          ) as revenue, -- NEW METRIC MOD: ADLF
       -- NEW METRIC MOD: Martin Alalu
       SUM(case when product = 'PAGO_CO' then total_interest ELSE 0 END) as total_interest_pago_co,
       SUM(case when requested_amount_without_discount > 0 and CAST(l.total_interest AS NUMERIC) > 0 and (product = 'PAGO_CO' 
                or credit_policy_name in ('addipago_0aprfga_policy', 'addipago_0fga_policy', 'addipago_claro_policy', 'addipago_mario_h_policy', 'addipago_no_history_policy', 'addipago_policy', 'addipago_policy_amoblando', 'adelante_policy_pago', 'closing_policy_pago', 'finalization_policy_pago', 'rc_0aprfga', 'rc_addipago_policy_amoblando', 'rc_closing_policy', 'rc_finalization_policy', 'rc_pago_0aprfga', 'rc_pago_0fga', 'rc_pago_claro', 'rc_pago_mario_h', 'rc_pago_standard', 'rc_rejection_policy')) then requested_amount_without_discount
                when (product = 'PAGO_CO' and CAST(l.total_interest AS NUMERIC) > 0
                or ((credit_policy_name in ('addipago_0aprfga_policy', 'addipago_0fga_policy', 'addipago_claro_policy', 'addipago_mario_h_policy', 'addipago_no_history_policy', 'addipago_policy', 'addipago_policy_amoblando', 'adelante_policy_pago', 'closing_policy_pago', 'finalization_policy_pago', 'rc_0aprfga', 'rc_addipago_policy_amoblando', 'rc_closing_policy', 'rc_finalization_policy', 'rc_pago_0aprfga', 'rc_pago_0fga', 'rc_pago_claro', 'rc_pago_mario_h', 'rc_pago_standard', 'rc_rejection_policy') and CAST(l.total_interest AS NUMERIC) > 0))) then requested_amount 
           end) as gmv_pago_flex_co,
       
       SUM(case when app.requested_amount_without_discount >0 AND m.application_id IS NOT NULL then app.requested_amount_without_discount
          WHEN m.application_id IS NOT NULL THEN app.requested_amount ELSE 0
     			end) as marketplace_gmv_co,
       SUM(case when m.application_id IS NOT NULL THEN 1 ELSE 0 END) as marketplace_originations_co,
       
       SUM(case when app.requested_amount_without_discount >0 AND la.ally_name IS NOT NULL AND m.application_id IS NOT NULL then app.requested_amount_without_discount
          WHEN m.application_id IS NOT NULL AND la.ally_name IS NOT NULL THEN app.requested_amount ELSE 0
      			end) as paying_merchants_gmv_co,
       
       SUM(case when la.ally_name IS NOT NULL AND m.application_id IS NOT NULL THEN 1 ELSE 0 END) as paying_merchants_originations_co,
       
       SUM(case when app.requested_amount_without_discount >0 AND la.ally_name IS NOT NULL AND m.application_id IS NOT NULL then app.requested_amount_without_discount * lead_gen_fee
          WHEN m.application_id IS NOT NULL AND la.ally_name IS NOT NULL THEN app.requested_amount * lead_gen_fee ELSE 0
      			end) as paying_merchants_fee_co,              
       
       
       SUM(case when requested_amount_without_discount > 0 and (product = 'PAGO_CO' 
                or credit_policy_name in ('addipago_0aprfga_policy', 'addipago_0fga_policy', 'addipago_claro_policy', 'addipago_mario_h_policy', 'addipago_no_history_policy', 'addipago_policy', 'addipago_policy_amoblando', 'adelante_policy_pago', 'closing_policy_pago', 'finalization_policy_pago', 'rc_0aprfga', 'rc_addipago_policy_amoblando', 'rc_closing_policy', 'rc_finalization_policy', 'rc_pago_0aprfga', 'rc_pago_0fga', 'rc_pago_claro', 'rc_pago_mario_h', 'rc_pago_standard', 'rc_rejection_policy')) then requested_amount_without_discount * guarantee_rate
                when (product = 'PAGO_CO' 
                or credit_policy_name in ('addipago_0aprfga_policy', 'addipago_0fga_policy', 'addipago_claro_policy', 'addipago_mario_h_policy', 'addipago_no_history_policy', 'addipago_policy', 'addipago_policy_amoblando', 'adelante_policy_pago', 'closing_policy_pago', 'finalization_policy_pago', 'rc_0aprfga', 'rc_addipago_policy_amoblando', 'rc_closing_policy', 'rc_finalization_policy', 'rc_pago_0aprfga', 'rc_pago_0fga', 'rc_pago_claro', 'rc_pago_mario_h', 'rc_pago_standard', 'rc_rejection_policy')) then requested_amount * guarantee_rate
           end) as fga_pago_co
       
       
from silver.f_originations_bnpl_co t
LEFT JOIN silver.f_underwriting_fraud_stage_co u
        ON t.application_id=u.application_id
LEFT JOIN silver.f_applications_co app
        ON app.application_id=t.application_id
LEFT JOIN silver.f_loan_proposals_co l
		ON t.loan_id = l.loan_proposal_id
LEFT JOIN  silver.f_marketplace_transaction_attributable_co m
		ON t.application_id = m.application_id
LEFT JOIN sandbox.marketplace_lead_gen_fee_allies_co la
    ON t.ally_slug = la.ally_name
    AND (from_utc_timestamp(t.origination_date,'America/Bogota'))::date >= start_date
AND (from_utc_timestamp(t.origination_date,'America/Bogota'))::date <= end_date
where (from_utc_timestamp(t.origination_date,'America/Bogota'))::date >= trunc(current_date(),'month') - interval '13 month'
group by 1,2,3,4,5,6
),

e2e as (
with t as (
select ocurred_on_timestamp, 
             client_id , 
             journey_name,
             ally_slug, 
             max(loan_acceptance_co_out) as loan_acceptance_co_out,
             max(loan_acceptance_V2_co_out) as loan_acceptance_V2_co_out,
             max(underwriting_co_out) as underwriting_co_out, -- NEW METRIC MOD: ADLF
             max(underwriting_co_in) as underwriting_co_in, -- NEW METRIC MOD: ADLF
             COALESCE(max(identity_verification_co_in),0) AS identity_verification_co_in
      from gold.dm_application_process_funnel_co
      where 1=1 and ocurred_on_date::date >= trunc(current_date(),'month') - interval '13 month'
      group by 1,2,3,4
      )
select  
		client_id,
        'CO' as country_code,
        ocurred_on_timestamp::date as period,
        date_trunc('week',ocurred_on_timestamp::date) as week,
        date_format(ocurred_on_timestamp::date, "yyyy-MM") as month,
        date_format(ocurred_on_timestamp::date, "yyyy-QQ") as quarter, 
        SUM(case when journey_name in ('CLIENT_PAGO_CO', 'CLIENT_FINANCIA_CO', 'CLIENT_CHECKOUT_PAGO_CO', 'CLIENT_PAGO_V2_CO') then 1 else 0 end) as rc_e2e_denominator,
        SUM(case when journey_name in ('CLIENT_PAGO_CO', 'CLIENT_FINANCIA_CO', 'CLIENT_CHECKOUT_PAGO_CO', 'CLIENT_PAGO_V2_CO') then GREATEST(t.loan_acceptance_co_out, t.loan_acceptance_V2_co_out) end) as rc_e2e_nominator,
        SUM(case when journey_name in ('CLIENT_PAGO_CO', 'CLIENT_CHECKOUT_PAGO_CO', 'CLIENT_PAGO_V2_CO') then 1 else 0 end) as rc_e2e_pago_co_denominator,
        SUM(case when journey_name in ('CLIENT_PAGO_CO', 'CLIENT_CHECKOUT_PAGO_CO', 'CLIENT_PAGO_V2_CO') then GREATEST(t.loan_acceptance_co_out, t.loan_acceptance_V2_co_out) end) as rc_e2e_pago_co_nominator,
        SUM(case when journey_name in ('PROSPECT_FINANCIA_CO', 'PROSPECT_CHECKPOINT_FINANCIA_CO', 'PROSPECT_FINANCIA_SANTANDER_CO') then 1 else 0 end) as prospect_e2e_financia_denominator, -- NEW METRIC MOD: ADLF
        SUM(case when journey_name in ('PROSPECT_FINANCIA_CO', 'PROSPECT_CHECKPOINT_FINANCIA_CO', 'PROSPECT_FINANCIA_SANTANDER_CO') then GREATEST(t.loan_acceptance_co_out, t.loan_acceptance_V2_co_out) end) as prospect_e2e_financia_nominator, -- NEW METRIC MOD: ADLF
        SUM(case when journey_name in ('PROSPECT_PAGO_CO', 'PROSPECT_CHECKPOINT_PAGO_CO', 'PROSPECT_PAGO_V2_CO', 'PROSPECT_CHECKPOINT_PAGO_V2_CO') then 1 else 0 end) as prospect_e2e_pago_co_denominator, -- NEW METRIC MOD: ADLF
        SUM(case when journey_name in ('PROSPECT_PAGO_CO', 'PROSPECT_CHECKPOINT_PAGO_CO', 'PROSPECT_PAGO_V2_CO', 'PROSPECT_CHECKPOINT_PAGO_V2_CO') then GREATEST(t.loan_acceptance_co_out, t.loan_acceptance_V2_co_out) end) as prospect_e2e_pago_co_nominator, -- NEW METRIC MOD: ADLF
        SUM(1) as e2e_denominator, -- NEW METRIC MOD: ADLF
        SUM(GREATEST(t.loan_acceptance_co_out, t.loan_acceptance_V2_co_out)) as e2e_nominator, -- NEW METRIC MOD: ADLF
        NULL AS prospect_e2e_denominator,
        NULL AS prospect_e2e_nominator,
        NULL AS prospect_e2e_wo_idv_denominator,
        NULL AS prospect_e2e_wo_idv_numerator,
        SUM(t.underwriting_co_out) filter (where journey_name not in ('CLIENT_PAGO_CO', 'CLIENT_FINANCIA_CO', 'CLIENT_CHECKOUT_PAGO_CO', 'CLIENT_PAGO_V2_CO')) as underwriting_nominator, -- NEW METRIC MOD: ADLF
        SUM(t.underwriting_co_in) filter (where journey_name not in ('CLIENT_PAGO_CO', 'CLIENT_FINANCIA_CO', 'CLIENT_CHECKOUT_PAGO_CO', 'CLIENT_PAGO_V2_CO')) as underwriting_denominator -- NEW METRIC MOD: ADLF
from t
group by 1,2,3,4,5,6
),

gmv_br as (

with t as (select client_id, application_id,loan_id,"BNPL" payment_type, origination_date from silver.f_originations_bnpl_br
UNION
select client_id,application_id,null as loan_id, "BNPN" payment_type,origination_date from silver.f_originations_bnpn_br )

select t.client_id,
      'BR' as country_code,
       (from_utc_timestamp(t.origination_date,'America/Sao_Paulo'))::date as period,
       date_trunc('week',(from_utc_timestamp(t.origination_date,'America/Sao_Paulo'))::date) as week,
       date_format((from_utc_timestamp(t.origination_date,'America/Sao_Paulo'))::date, "yyyy-MM") as month,
       date_format((from_utc_timestamp(t.origination_date,'America/Sao_Paulo'))::date, "yyyy-QQ") as quarter,
       SUM(case when requested_amount_without_discount >0 then requested_amount_without_discount
                else requested_amount 
           end) as gmv,
       SUM(case when  payment_type = 'BNPN' and requested_amount_without_discount > 0 then requested_amount_without_discount
                when  payment_type = 'BNPN' then requested_amount 
           end) as gmv_through_bnpn_pix,
       SUM(case when  payment_type <> 'BNPN' and requested_amount_without_discount > 0 then requested_amount_without_discount
                when  payment_type <> 'BNPN' then requested_amount 
           end) as gmv_pago_br,    
       SUM(1) as transactions,
       SUM(case when payment_type = 'BNPN' AND (client_type != 'CLIENT' OR client_type IS NULL)  then 1
           end) as transactions_through_bnpn_pix_prospect,
       SUM(case when payment_type = 'BNPN' AND (client_type = 'CLIENT') then 1
           end) as transactions_through_bnpn_pix_rc,
       SUM(case when payment_type <> 'BNPN' AND (client_type != 'CLIENT' OR client_type IS NULL) then 1
           end) as transactions_pago_br_prospect,
       SUM(case when payment_type <> 'BNPN' AND (client_type = 'CLIENT') then 1
           end) as transactions_pago_br_rc,
       SUM(case when  payment_type = 'BNPN' and client_type = 'PROSPECT' then 1 
           end) as originations_through_bnpn_pix,
       SUM(case when  payment_type <> 'BNPN' then 1 
           end) as originations_pago_br,  
       SUM((case when requested_amount_without_discount >0 then requested_amount_without_discount
                      else requested_amount 
                 end)*ally_mdf
          ) as dmf_nominator,
       SUM(case when requested_amount_without_discount > 0 then requested_amount_without_discount 
                else requested_amount 
           end) as dmf_denominator, --100 as weigthed_DMF
       SUM((case when  payment_type <> 'BNPN' and requested_amount_without_discount > 0 then requested_amount_without_discount
                when  payment_type <> 'BNPN' then requested_amount 
           end)*ally_mdf 
          ) as mdf_nominator_pago_br,   
       SUM((case when requested_amount_without_discount >0 or requested_amount_without_discount is distinct from '' then requested_amount_without_discount
                      else requested_amount 
                 end)*ally_mdf
          ) as revenue, -- NEW METRIC MOD: ADLF
       -- NEW METRIC MOD: Martin Alalu
       SUM(case when product = 'PAGO_BR' then total_interest ELSE 0 END) as total_interest_pago_br,
       SUM(case when  payment_type <> 'BNPN' and CAST(l.total_interest AS NUMERIC) > 0 and requested_amount_without_discount > 0 then requested_amount_without_discount
                when  payment_type <> 'BNPN' and CAST(l.total_interest AS NUMERIC) > 0 then requested_amount 
           end) as gmv_pago_flex_br,           
       SUM(case when requested_amount_without_discount >0 AND t.application_id IS NOT NULL then requested_amount_without_discount
                when t.application_id IS NOT NULL THEN requested_amount ELSE 0 end) as marketplace_gmv_br,
       SUM(case when t.application_id IS NOT NULL THEN 1 ELSE 0 END) as marketplace_originations_br,
       0 as paying_merchants_gmv_br,
       0 as paying_merchants_originations_br,
       0 as paying_merchants_fee_br,
       0 as fga_pago_br   
       
from t
LEFT JOIN silver.f_underwriting_fraud_stage_br u
        ON t.application_id=u.application_id
LEFT JOIN silver.f_applications_br app
        ON app.application_id=t.application_id
LEFT JOIN silver.f_loan_proposals_br l
		ON t.loan_id = l.loan_proposal_id and t.loan_id is not null
LEFT JOIN  silver.f_marketplace_transaction_attributable_br m
		ON t.application_id = m.application_id
where from_utc_timestamp(t.origination_date,'America/Sao_Paulo')::date >= trunc(current_date(),'month') - interval '13 month'
group by 1,2,3,4,5,6
),

e2e_br (
    with t as (select ocurred_on_timestamp, 
             client_id , 
             journey_name,
             ally_slug, 
             max(loan_acceptance_br_out) as loan_acceptance_br_out,
             max(banking_license_partner_br_out) as banking_license_partner_br_out,
             max(bn_pn_payments_br_out) as bn_pn_payments_br_out, -- NEW METRIC MOD: ADLF
             max(underwriting_br_out) as underwriting_br_out, -- NEW METRIC MOD: ADLF
             max(underwriting_br_in) as underwriting_br_in, -- NEW METRIC MOD: ADLF
             COALESCE(max(identity_verification_br_in),0) AS identity_verification_br_in
      from gold.dm_application_process_funnel_br
      where 1=1
      and ocurred_on_date::date >= trunc(current_date(),'month') - interval '13 month'
      group by 1,2,3,4
      )
select  client_id,
        'BR' as country_code,
        ocurred_on_timestamp::date as period,
        date_trunc('week',ocurred_on_timestamp::date) as week,
        date_format(ocurred_on_timestamp::date, "yyyy-MM") as month,
        date_format(ocurred_on_timestamp::date, "yyyy-QQ") as quarter, 
        SUM(case when journey_name = 'CLIENT_PAGO_BR' then 1 else 0 end) as rc_e2e_denominator,
        SUM(case when journey_name = 'CLIENT_PAGO_BR' AND ocurred_on_timestamp >= TIMESTAMP '2021-09-07 04:15:36' then t.banking_license_partner_br_out  
                 when journey_name = 'CLIENT_PAGO_BR' AND ocurred_on_timestamp < TIMESTAMP '2021-09-07 04:15:36' then t.loan_acceptance_br_out end) as rc_e2e_nominator,
        SUM(case when journey_name = 'CLIENT_PAGO_BR' then t.bn_pn_payments_br_out end) as rc_e2e_pix_nominator, -- NEW METRIC MOD: ADLF
        SUM(1) as e2e_denominator, -- NEW METRIC MOD: ADLF
        SUM(case when ocurred_on_timestamp >= TIMESTAMP '2021-09-07 04:15:36' then t.banking_license_partner_br_out  
                 when ocurred_on_timestamp < TIMESTAMP '2021-09-07 04:15:36' then t.loan_acceptance_br_out end) as e2e_nominator, -- NEW METRIC MOD: ADLF
        SUM(case when journey_name IN ('PROSPECT_PAGO_BR','PROSPECT_CHECKPOINT_PAGO_BR','PROSPECT_PAGO_V2_BR') then 1 else 0 end) as prospect_e2e_denominator,
        SUM(case when journey_name IN ('PROSPECT_PAGO_BR','PROSPECT_CHECKPOINT_PAGO_BR','PROSPECT_PAGO_V2_BR') AND ocurred_on_timestamp >= TIMESTAMP '2021-09-07 04:15:36' then t.banking_license_partner_br_out  
                 when journey_name IN ('PROSPECT_PAGO_BR','PROSPECT_CHECKPOINT_PAGO_BR','PROSPECT_PAGO_V2_BR') AND ocurred_on_timestamp < TIMESTAMP '2021-09-07 04:15:36' then t.loan_acceptance_br_out end) as prospect_e2e_nominator,
        SUM(case when journey_name IN ('PROSPECT_PAGO_BNPN_BR_LEGACY') then t.bn_pn_payments_br_out end) as prospect_e2e_pix_nominator,
        SUM(case when journey_name IN ('PROSPECT_PAGO_BNPN_BR_LEGACY') then 1 else 0 end) as prospect_e2e_pix_denominator,
        SUM(case when journey_name IN ('PROSPECT_PAGO_BR','PROSPECT_CHECKPOINT_PAGO_BR','PROSPECT_PAGO_V2_BR') AND ocurred_on_timestamp >= TIMESTAMP '2021-09-07 04:15:36' AND identity_verification_br_in=0 then t.banking_license_partner_br_out  
                 when journey_name IN ('PROSPECT_PAGO_BR','PROSPECT_CHECKPOINT_PAGO_BR','PROSPECT_PAGO_V2_BR') AND ocurred_on_timestamp < TIMESTAMP '2021-09-07 04:15:36' AND identity_verification_br_in=0 then t.loan_acceptance_br_out 
                 ELSE 0 end) as prospect_e2e_wo_idv_numerator, -- NEW METRIC MOD: CDAPN
		SUM(case when journey_name IN ('PROSPECT_PAGO_BR','PROSPECT_CHECKPOINT_PAGO_BR','PROSPECT_PAGO_V2_BR') AND identity_verification_br_in=0 then 1
				 ELSE 0 end) as prospect_e2e_wo_idv_denominator, -- NEW METRIC MOD: CDAPN
        SUM(t.underwriting_br_out) as underwriting_nominator, -- NEW METRIC MOD: ADLF
        SUM(t.underwriting_br_in) as underwriting_denominator -- NEW METRIC MOD: ADLF
from t
group by 1,2,3,4,5,6
)

select coalesce(e.client_id, g.client_id) as client_id,
       coalesce(e.country_code, g.country_code) as country_code,
       coalesce(e.period,g.period) as period,
       coalesce(e.week, g.week) as week,
       coalesce(e.month, g.month) as month,
       coalesce(e.quarter, g.quarter) as quarter,
       rc_e2e_denominator,
       rc_e2e_nominator,
       rc_e2e_pago_co_denominator,
       rc_e2e_pago_co_nominator,
       prospect_e2e_pago_co_denominator,
       prospect_e2e_pago_co_nominator,
       prospect_e2e_financia_denominator,
       prospect_e2e_financia_nominator,
       null as rc_e2e_pix_nominator,
       e2e_denominator,
       e2e_nominator,
       null as prospect_e2e_denominator,
       null as prospect_e2e_nominator,
       null as prospect_e2e_pix_denominator,
       null as prospect_e2e_pix_nominator,
       NULL AS prospect_e2e_wo_idv_denominator,
       NULL AS prospect_e2e_wo_idv_numerator,
       gmv,
       gmv_pago_co,
       gmv_financia_co,
       0 as gmv_pago_br,
       0 as gmv_through_bnpn_pix,
       transactions,
       transactions_pago_co_rc,
       transactions_pago_co_prospect,
       transactions_financia_co_rc,
       transactions_financia_co_prospect,
       null AS transactions_through_bnpn_pix_prospect,
       null AS transactions_through_bnpn_pix_rc,
       null AS transactions_pago_br_prospect,
       null AS transactions_pago_br_rc,
       originations_pago_co,
       originations_financia_co,
       0 as originations_pago_br,
       0 as originations_through_bnpn_pix,
       dmf_denominator,
       dmf_nominator,
       0 as mdf_nominator_pago_br,
       mdf_nominator_pago_co,
       underwriting_denominator,
       underwriting_nominator, 
       revenue,
       total_interest_pago_co,
       0 as total_interest_pago_br,
       gmv_pago_flex_co,
       0 as gmv_pago_flex_br,
       marketplace_gmv_co,
       0 as marketplace_gmv_br,
       marketplace_originations_co,
       0 as marketplace_originations_br,
       paying_merchants_gmv_co,
       0 as paying_merchants_gmv_br,
       paying_merchants_originations_co,
       0 as paying_merchants_originations_br,
       paying_merchants_fee_co,
       0 paying_merchants_fee_br,
       fga_pago_co,
       0 as fga_pago_br
from e2e e
full join gmv g on g.client_id = e.client_id and g.period = e.period
union all
select coalesce(e.client_id, g.client_id) as client_id,
       coalesce(e.country_code, g.country_code) as country_code,
       coalesce(e.period,g.period) as period,
       coalesce(e.week, g.week) as week,
       coalesce(e.month, g.month) as month,
       coalesce(e.quarter, g.quarter) as quarter,
       rc_e2e_denominator,
       rc_e2e_nominator,
       NULL AS rc_e2e_pago_co_denominator,
       NULL AS rc_e2e_pago_co_nominator,
       NULL AS prospect_e2e_pago_co_denominator,
       NULL AS prospect_e2e_pago_co_nominator,
       NULL AS prospect_e2e_financia_denominator,
       NULL AS prospect_e2e_financia_nominator,
       rc_e2e_pix_nominator,
       e2e_denominator,
       e2e_nominator,
       prospect_e2e_denominator,
       prospect_e2e_nominator,
       prospect_e2e_pix_denominator,
       prospect_e2e_pix_nominator,
       prospect_e2e_wo_idv_denominator,
       prospect_e2e_wo_idv_numerator,
       gmv,
       0 as gmv_pago_co,
       0 as gmv_financia_co,
       gmv_pago_br,
       gmv_through_bnpn_pix,
       transactions,
       null AS transactions_pago_co_rc,
       null AS transactions_pago_co_prospect,
       null AS transactions_financia_co_rc,
       null AS transactions_financia_co_prospect,
       transactions_through_bnpn_pix_prospect,
       transactions_through_bnpn_pix_rc,
       transactions_pago_br_prospect,
       transactions_pago_br_rc,
       0 as originations_pago_co,
       0 as originations_financia_co,
       originations_pago_br,
       originations_through_bnpn_pix,
       dmf_denominator,
       dmf_nominator,
       mdf_nominator_pago_br,
       0 as mdf_nominator_pago_co,
       underwriting_denominator,
       underwriting_nominator,
       revenue,
       0 as total_interest_pago_co,       
       total_interest_pago_br,
       0 as gmv_pago_flex_co,
       gmv_pago_flex_br,
       0 as marketplace_gmv_co,
       marketplace_gmv_br,
       0 as marketplace_originations_co,
       marketplace_originations_br,
       0 paying_merchants_gmv_co,
       paying_merchants_gmv_br,
       0 as paying_merchants_originations_co,
       paying_merchants_originations_br,
       0 as paying_merchants_fee_co,
       paying_merchants_fee_br,
       0 as fga_pago_co,
       fga_pago_br
from e2e_br e
full join gmv_br g on g.client_id = e.client_id and g.period = e.period