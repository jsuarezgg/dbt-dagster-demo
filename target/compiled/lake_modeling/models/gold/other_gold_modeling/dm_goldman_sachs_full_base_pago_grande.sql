

WITH cupo_grande_loans AS (
  SELECT 
    a.*
    , b.product
    , d.ally_mdf
    , CASE WHEN b.preapproval_client = 'Preapproval' then true else false end as preapproval_application
    , CASE WHEN c.loan_ownership = 'PA_ADDI_GOLDMAN' and c.sale_date is not null then true else false end as sold_to_gs
    , CASE WHEN c.loan_ownership = 'PA_ADDI_GOLDMAN' and c.sale_date is not null then c.sale_date end as sell_date

-- DPD x+ Date
    , b.FP_date_plus_5_day
    , b.FP_date_plus_10_day
    , b.FP_date_plus_15_day
    , b.FP_date_plus_1_month -- 31
    , b.FP_date_plus_2_month -- 61
    , b.FP_date_plus_3_month -- 91
    , b.FP_date_plus_4_month -- 121
    , b.FP_date_plus_5_month -- 151
    , b.FP_date_plus_6_month -- 181
    , b.FP_date_plus_7_month -- 211
    , b.FP_date_plus_8_month -- 241
    , b.FP_date_plus_9_month -- 271
    , b.FP_date_plus_10_month -- 301
    , b.FP_date_plus_11_month -- 331
    , b.FP_date_plus_17_month -- 511

-- DPD x+    
    , b.DPD_plus_5_day
    , b.DPD_plus_10_day
    , b.DPD_plus_15_day
    , b.DPD_plus_1_month -- 31
    , b.DPD_plus_2_month -- 61
    , b.DPD_plus_3_month -- 91
    , b.DPD_plus_4_month -- 121
    , b.DPD_plus_5_month -- 151
    , b.DPD_plus_6_month -- 181
    , b.DPD_plus_7_month -- 211
    , b.DPD_plus_8_month -- 241
    , b.DPD_plus_9_month -- 271
    , b.DPD_plus_10_month -- 301
    , b.DPD_plus_11_month -- 331
    , b.DPD_plus_17_month -- 511

-- Unpaid Principal DPDx+    
    , b.UPB_plus_5_day
    , b.UPB_plus_10_day
    , b.UPB_plus_15_day
    , b.UPB_plus_1_month -- 31
    , b.UPB_plus_2_month -- 61
    , b.UPB_plus_3_month -- 91
    , b.UPB_plus_4_month -- 121
    , b.UPB_plus_5_month -- 151
    , b.UPB_plus_6_month -- 181
    , b.UPB_plus_7_month -- 211
    , b.UPB_plus_8_month -- 241
    , b.UPB_plus_9_month -- 271
    , b.UPB_plus_10_month -- 301
    , b.UPB_plus_11_month -- 331
    , b.UPB_plus_17_month -- 511
    
  FROM silver.d_snc_client_loan_calculations_co a
  LEFT JOIN gold.risk_master_table_co b 
    ON a.loan_id = b.loan_id
  LEFT JOIN gold.loan_ownership_co c
    ON a.loan_id = c.loan_id
  LEFT JOIN gold.dm_loan_status_co d 
    ON a.loan_id = d.loan_id
  WHERE b.synthetic_product_category IN ('CUPO', 'GRANDE')
    AND a.origination_date >= '2020-04-01' --New Book
)

-- 2. Create list of loans that have been modified.
-- Loans with modifications must NOT be sold
, loan_modifications AS (
  SELECT
    DISTINCT loan_id
  FROM silver.f_syc_refinancing_instructions_co
  UNION ALL
  SELECT 
    DISTINCT loan_id
  FROM silver.f_syc_condonations_co where type in ('CONDONATION_BY_CLIENT') and loan_id not in (SELECT DISTINCT loan_id FROM silver.f_syc_refinancing_instructions_co)
)

, addi_employees_id AS (
  SELECT
    DISTINCT a.client_id
  FROM silver.f_pii_applications_co a
  INNER JOIN silver.d_employees_id_co b
    ON a.id_type = b.id_type
    AND a.id_number = b.employee_id
)

-- 4. Create list of IA loans after incorporation into regular credit policy (not tagged AS Learning Population)
, ia_scoring AS (
  SELECT 
    application_id
    ,final_pd_score AS final_pd
    ,original_pd_score AS original_pd
  FROM gold.rmt_pd_models_co
)
, ia_application_ids AS (
  SELECT 
    application_id
  FROM ia_scoring
  where final_pd < original_pd --IA loans have a lower final PD
)

-- 5. Create max dpd list by loan
-- Check max days past due per loan
, max_dpd_loan AS (
  SELECT
    a.loan_id
    ,max(a.days_past_due) AS max_dpd
  FROM gold.dm_daily_loan_status_co a
  LEFT JOIN cupo_grande_loans b on a.loan_id = b.loan_id
  WHERE b.loan_id IS NOT NULL --Only get data for Financia And Cupo loans
  GROUP BY 1
)

-- 6. Create max dpd list by client
-- Check max days past due per client
, max_dpd_client AS (
  SELECT
    a.client_id
    ,max(a.days_past_due) AS client_max_dpd
  FROM gold.dm_daily_loan_status_co a
  GROUP BY 1
)


-- 7. Consolidate everything into a single loan tape
, loan_tape AS (

SELECT DISTINCT
  a.loan_id
  ,a.product
  ,udw.credit_score
  ,udw.credit_score_name AS credit_score_product
  ,udw.tdsr
  ,a.ally_mdf AS merchant_discount_fee
  ,kyc_ie.income_estimatedIncome AS estimated_income
  ,udw.credit_check_income_provider
  ,rmt.ally_region AS region
  ,udw.probability_default_addi AS addi_pd
  ,CASE WHEN d.loan_id is not null then true else false end AS foreberance
  ,CASE WHEN rmt.client_type = 'CLIENT' then true else false end AS returning_client --CHECK THIS IS WORKING FOR HISTORICAL LOANS
  ,e.max_dpd
  ,CASE
    when udw.learning_population is true then 'Learning Population'
    when udw.lbl is true then 'ADDIFinancia Low Balance Loan'
--     when f.application_id is not null then 'ADDIFinancia IA regular credit policy' -- Request to be turned off by Finance Team 9-8-22
    when f.application_id is not null then 'ADDIFinancia Core' -- Formerly IA regular credit policy
    when rmt.interest_rate::double =0 then 'ADDIFinancia 0% APR' --(1)
    else 'ADDIFinancia Core'
  END AS product_type
  ,a.client_id
  ,a.applicable_rate AS current_apr -- (2)
  ,rmt.interest_rate::double AS origination_apr --(3)
  ,coalesce(udw.lbl, false) AS low_balance_loan
  ,CASE
    when udw.lbl is true then g.official_low_balance_usury_rate 
    else g.official_usury_rate 
  END AS current_usury_rate

  ,(a.first_payment_date + interval '1 month' * (rmt.term - 1))::date AS expected_maturity_date
  ,a.current_installment_amount
  ,rmt.ally_name
  ,rmt.ally_vertical
  ,a.is_fully_paid
  ,CASE WHEN a.cancellation_reason is not null then true else false end AS is_cancelled
  ,i.client_max_dpd
  ,kyc_b.personId_ageRange AS obligor_age_range
  ,a.approved_amount
  ,a.cancellation_reason
  ,a.first_payment_date::date
  ,CASE 
    WHEN a.calculation_date < a.first_payment_date then 0
    else (case  when extract(day from a.calculation_date) > extract(day from a.first_payment_date) 
      then (extract(year from a.calculation_date)*12+extract(month from a.calculation_date)) 
    - (extract(year from a.first_payment_date)*12+extract(month from a.first_payment_date)) +1
      else (extract(year from a.calculation_date)*12+extract(month from a.calculation_date)) 
    - (extract(year from a.first_payment_date)*12+extract(month from a.first_payment_date)) end)
   end AS mob
  ,a.origination_date::date
  ,rmt.term
  ,a.unpaid_principal
  ,CASE WHEN f.application_id is not null then true else false end AS is_ia_loan
  ,coalesce(udw.learning_population, false) AS learning_population
  ,a.days_past_due
  ,CASE WHEN j.client_id is not null then true else false end AS is_addi_employee
  ,coalesce(a.preapproval_application, false) AS is_preapproval
  ,rmt.ally_brand

  -- DPD x+ Date
    , a.FP_date_plus_5_day
    , a.FP_date_plus_10_day
    , a.FP_date_plus_15_day
    , a.FP_date_plus_1_month -- 31
    , a.FP_date_plus_2_month -- 61
    , a.FP_date_plus_3_month -- 91
    , a.FP_date_plus_4_month -- 121
    , a.FP_date_plus_5_month -- 151
    , a.FP_date_plus_6_month -- 181
    , a.FP_date_plus_7_month -- 211
    , a.FP_date_plus_8_month -- 241
    , a.FP_date_plus_9_month -- 271
    , a.FP_date_plus_10_month -- 301
    , a.FP_date_plus_11_month -- 331
    , a.FP_date_plus_17_month -- 511

-- DPD x+    
    , a.DPD_plus_5_day
    , a.DPD_plus_10_day
    , a.DPD_plus_15_day
    , a.DPD_plus_1_month -- 31
    , a.DPD_plus_2_month -- 61
    , a.DPD_plus_3_month -- 91
    , a.DPD_plus_4_month -- 121
    , a.DPD_plus_5_month -- 151
    , a.DPD_plus_6_month -- 181
    , a.DPD_plus_7_month -- 211
    , a.DPD_plus_8_month -- 241
    , a.DPD_plus_9_month -- 271
    , a.DPD_plus_10_month -- 301
    , a.DPD_plus_11_month -- 331
    , a.DPD_plus_17_month -- 511

-- Unpaid Principal DPDx+    
    , a.UPB_plus_5_day
    , a.UPB_plus_10_day
    , a.UPB_plus_15_day
    , a.UPB_plus_1_month -- 31
    , a.UPB_plus_2_month -- 61
    , a.UPB_plus_3_month -- 91
    , a.UPB_plus_4_month -- 121
    , a.UPB_plus_5_month -- 151
    , a.UPB_plus_6_month -- 181
    , a.UPB_plus_7_month -- 211
    , a.UPB_plus_8_month -- 241
    , a.UPB_plus_9_month -- 271
    , a.UPB_plus_10_month -- 301
    , a.UPB_plus_11_month -- 331
    , a.UPB_plus_17_month -- 511
    
  ,a.calculation_date::date AS calculation_date
  ,a.sold_to_gs
  ,a.sell_date
  
FROM cupo_grande_loans a
LEFT JOIN silver.f_originations_bnpl_co b on a.loan_id = b.loan_id
LEFT JOIN silver.f_underwriting_fraud_stage_co udw on b.application_id = udw.application_id
LEFT JOIN gold.risk_master_table_co rmt on a.loan_id = rmt.loan_id
LEFT JOIN silver.f_kyc_bureau_income_estimator_co kyc_ie on b.application_id = kyc_ie.application_id
LEFT JOIN silver.f_kyc_bureau_personal_info_co kyc_b on b.application_id = kyc_b.application_id
LEFT JOIN loan_modifications d on a.loan_id = d.loan_id
LEFT JOIN max_dpd_loan e on a.loan_id = e.loan_id
LEFT JOIN ia_application_ids f on b.application_id = f.application_id
LEFT JOIN silver.d_syc_usury_rates_co g on a.calculation_date between g.start_date and g.end_date
LEFT JOIN max_dpd_client i on a.client_id = i.client_id
LEFT JOIN addi_employees_id j on a.client_id = j.client_id  
)

select --*
  a.loan_id
  ,a.product
  ,a.credit_score
  ,a.credit_score_product
  ,a.tdsr
  ,a.merchant_discount_fee
  ,a.estimated_income
  ,a.credit_check_income_provider
  ,a.region
  ,a.addi_pd
  ,a.foreberance
  ,a.returning_client
  ,a.max_dpd
  ,a.product_type
  ,a.client_id
  ,a.current_apr
  ,a.origination_apr
  ,a.low_balance_loan
  ,a.current_usury_rate
  ,a.expected_maturity_date
  ,a.current_installment_amount
  ,a.ally_name
  ,a.ally_vertical
  ,a.is_fully_paid
  ,a.is_cancelled
  ,a.client_max_dpd
  ,a.obligor_age_range
  ,a.approved_amount
  ,a.cancellation_reason
  ,a.first_payment_date
  ,a.mob
  ,a.origination_date
  ,a.term
  ,a.unpaid_principal
  ,a.days_past_due
  ,a.is_addi_employee
  ,a.sell_date
  ,b.id_number

from loan_tape a
left join gold.fraud_master_table_co b on a.loan_id = b.loan_id --Add ID number
where a.sold_to_gs = true;