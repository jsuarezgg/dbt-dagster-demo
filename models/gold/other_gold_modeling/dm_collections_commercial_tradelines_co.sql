{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH tradelines_data AS (
  SELECT
    client_id,
    bureau_tradeline_originationDate AS tradeline_origination_date,
    bureau_tradeline_obligationNumber AS tradeline_obligation_number,
    bureau_tradeline_entityName AS tradeline_entity_name,
    datediff(bureau_tradeline_terminationDate, bureau_tradeline_originationDate) / 30 AS tradeline_term_approx,
    bureau_tradeline_city AS tradeline_city,
    bureau_tradeline_branch AS tradeline_branch,
    bureau_tradeline_obligationType AS tradeline_obligation_type,
    bureau_tradeline_lastUpdateDate AS tradeline_obligation_status_last_update_date
  FROM  {{ source('silver_live', 'f_kyc_bureau_credit_history_tradelines_co_logs') }}
)
,
tradelines_df AS (
  SELECT
    client_id,
    collect_list(tradeline_origination_date) AS tradeline_origination_date,
    collect_list(tradeline_obligation_number) AS tradeline_obligation_number,
    collect_list(tradeline_entity_name) AS tradeline_entity_name,
    collect_list(tradeline_term_approx) AS tradeline_term_approx,
    collect_list(tradeline_city) AS tradeline_city,
    collect_list(tradeline_branch) AS tradeline_branch,
    collect_list(tradeline_obligation_type) AS tradeline_obligation_type,
    collect_list(tradeline_obligation_status_last_update_date) AS tradeline_obligation_status_last_update_date
  FROM tradelines_data
  GROUP BY 1
)
,
footprints_df AS (
  SELECT
    client_id,
    collect_list(1) AS footprint_entity_name,
    collect_list(1) AS footprint_ciy,
    collect_list(1) AS footprint_date,
    collect_list(1) AS footprint_branch
  FROM  {{ source('silver_live', 'f_kyc_bureau_credit_history_bank_accounts_co_logs') }}
  GROUP BY 1
)
,
bank_accounts_data_df AS (
  SELECT
    client_id,
    collect_list(bureau_bank_account_entityName) AS bank_entity_name,
    collect_list(bureau_bank_account_branch) AS bank_branch,
    collect_list(bureau_bank_account_obligationType) AS bank_obligation_type,
    collect_list(bureau_bank_account_originationDate) AS bank_origination_date,
    collect_list(bureau_bank_account_lastUpdateDate) AS bank_last_update_date
  FROM  {{ source('silver_live', 'f_kyc_bureau_credit_history_bank_accounts_co_logs') }}
  GROUP BY 1
)
,
income_estimator_data_df AS (
  SELECT
    client_id,
    collect_list(income_estimatedIncome) AS estimated_income,
    collect_list(income_indebtednessCapacity) AS indebtedness_capacity,
    collect_list(income_totalActiveProducts) AS total_active_products,
    collect_list(income_totalInitialApprovedAmount) AS total_initial_approved_amount,
    collect_list(income_totalBalance) AS total_balance,
    collect_list(income_totalInstallment) AS total_installment,
    collect_list(income_nonRevolvingTotalProducts) AS non_revolving_total_products,
    collect_list(income_nonRevolvingTotalInitialApprovedAmount) AS non_revolving_total_initiaL_approved_amount,
    collect_list(income_nonRevolvingTotalBalance) AS non_revolving_total_balance,
    collect_list(income_nonRevolvingTotalInstallment) AS non_revolving_total_installment,
    collect_list(income_totalActiveCreditCards) AS total_active_credit_cards,
    collect_list(income_creditCardInitialApprovedAmount) AS credit_card_initial_approved_amount,
    collect_list(income_creditCardBalance) AS credit_card_balance,
    collect_list(income_creditCardInstallment) AS credit_card_installment
  FROM  {{ source('silver_live', 'f_kyc_bureau_income_estimator_co') }}
  GROUP BY 1
)
,
income_validator_data_df AS (
  SELECT
    client_id,
    collect_list(income_healthEntity) AS health_entity,
    collect_list(income_pensionFundName) AS pension_fund_name
  FROM  {{ source('silver_live', 'f_kyc_bureau_income_validator_co') }}
  GROUP BY 1
)
SELECT
  COALESCE(trad.client_id, foot.client_id, bank.client_id, inc_es.client_id, inc_val.client_id) AS client_id,
  trad.tradeline_origination_date,
  trad.tradeline_obligation_number,
  trad.tradeline_entity_name,
  trad.tradeline_term_approx,
  trad.tradeline_city,
  trad.tradeline_branch,
  trad.tradeline_obligation_type,
  trad.tradeline_obligation_status_last_update_date,
  foot.footprint_entity_name,
  foot.footprint_ciy,
  foot.footprint_date,
  foot.footprint_branch,
  bank.bank_entity_name,
  bank.bank_branch,
  bank.bank_obligation_type,
  bank.bank_origination_date,
  bank.bank_last_update_date,
  inc_es.estimated_income,
  inc_es.indebtedness_capacity,
  inc_es.total_active_products,
  inc_es.total_initial_approved_amount,
  inc_es.total_balance,
  inc_es.total_installment,
  inc_es.non_revolving_total_products,
  inc_es.non_revolving_total_initiaL_approved_amount,
  inc_es.non_revolving_total_balance,
  inc_es.non_revolving_total_installment,
  inc_es.total_active_credit_cards,
  inc_es.credit_card_initial_approved_amount,
  inc_es.credit_card_balance,
  inc_es.credit_card_installment,
  inc_val.health_entity,
  inc_val.pension_fund_name
FROM tradelines_df trad
LEFT JOIN footprints_df foot                ON trad.client_id = foot.client_id
LEFT JOIN bank_accounts_data_df bank        ON trad.client_id = bank.client_id
LEFT JOIN income_estimator_data_df inc_es   ON trad.client_id = inc_es.client_id
LEFT JOIN income_validator_data_df inc_val  ON trad.client_id = inc_val.client_id
