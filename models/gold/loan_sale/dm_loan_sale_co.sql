{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH 

d_employees_id_co AS (

    SELECT * FROM {{ ref('d_employees_id_co') }}

),

f_syc_refinancing_instructions_co AS (

    SELECT * FROM {{ ref('f_syc_refinancing_instructions_co') }}

),

d_prospect_personal_data_co AS (

    SELECT * FROM {{ source('silver_live', 'd_prospect_personal_data_co') }}

),

f_syc_condonations_co AS (

    SELECT * FROM {{ ref('f_syc_condonations_co') }}

),

d_snc_client_loan_calculations_co AS (

    SELECT * FROM {{ ref('d_snc_client_loan_calculations_co') }}

),

dm_originations_base AS (

    SELECT * FROM {{ ref('dm_originations') }}

),

d_syc_usury_rates_co AS (

    SELECT * FROM {{ ref('d_syc_usury_rates_co') }}

),

loan_ownership_co AS (

    SELECT * FROM {{ ref('loan_ownership_co') }}

),

pap_exclusion_loan_sale AS (

    SELECT * FROM {{ source('risk', 'pap_exclusion_loan_sale') }}

),

dm_daily_loan_status_co AS (

    SELECT * FROM {{ source('gold', 'dm_daily_loan_status_co') }}

),

reason_to_filter_loans AS (

    SELECT * FROM {{ source('risk', 'reason_to_filter_loans') }}

),

dm_grande_lbl_eligibility_criteria AS (

    SELECT
        loan_id,
        COALESCE(number_of_financial_tradelines, 0) AS number_of_financial_tradelines,
        COALESCE(number_of_financial_lbl_tradelines, 0) AS number_of_financial_lbl_tradelines,
        COALESCE(total_lbl_amount, 0) AS total_lbl_amount
    FROM hive_metastore.ds.dm_grande_lbl_eligibility_criteria

),

f_refinance_loans_co AS (

    SELECT * FROM {{ source('silver_live', 'f_refinance_loans_co') }}

),

f_kyc_bureau_income_estimator_co AS (

    SELECT application_id, income_estimatedIncome FROM {{ source('silver_live', 'f_kyc_bureau_income_estimator_co') }}

),

f_kyc_bureau_personal_info_co AS (

    SELECT application_id, personId_ageRange FROM {{ source('silver_live', 'f_kyc_bureau_personal_info_co') }}

),

f_underwriting_fraud_stage_co AS (

    SELECT application_id, credit_check_income_provider, tdsr FROM {{ source('silver_live', 'f_underwriting_fraud_stage_co') }}

),

dm_loan_cancellations_co AS (

SELECT
    loan_id,
    last_total_cancellation_reason,
    custom_is_totally_cancelled,
    custom_has_partial_cancellations
FROM {{ ref('dm_loan_cancellations_co') }}

),

dm_condonations AS (
SELECT loan_id,
       condonation_date,
       CASE WHEN condonation_reason = 'FGA_CLAIM' THEN true
            ELSE false
       END AS is_fga_claimed,
       row_number() OVER(PARTITION BY loan_id ORDER BY condonation_date DESC) AS row_num  
FROM {{ ref('dm_condonations') }}
QUALIFY row_num = 1
),

risk_master_table AS (

SELECT
    rmt.application_id,
    rmt.addi_pd,
    rmt.ally_is_terminated,
    COALESCE(rmt.mdf, 0) AS mdf,
    rmt.ally_name,
    rmt.ally_slug,
    rmt.ally_vertical,
    rmt.ally_region,
    rmt.approved_amount,
    rmt.credit_score,
    rmt.credit_score_name,
    rmt.client_type,
    rmt.days_past_due,
    kyc_ie.income_estimatedIncome as estimated_income,
    udw.credit_check_income_provider,
    (rmt.first_payment_date + interval '1 month' * (rmt.term - 1))::date AS expected_maturity_date,
    rmt.first_payment_date,
    rmt.ia_loan,
    rmt.is_fully_paid,
    CASE WHEN rl.loan_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_refinanced_loan,
    if(rmt.loan_number >= 2, TRUE, FALSE) AS is_returning_client,
    coalesce(paps.identification, rmt.learning_population, false) AS learning_population,
    rmt.loan_id,
    rmt.loan_number,
    rmt.low_balance_loan,
    rmt.mob,
    kyc_b.personId_ageRange as obligor_age_range,
    rmt.paid_installments,
    rmt.product,
    rmt.prospect_age_avg,
    rmt.synthetic_product_category,
    rmt.synthetic_product_subcategory,
    rmt.term,
    udw.tdsr,
    rmt.unpaid_principal
FROM {{ ref('risk_master_table_co') }} rmt
LEFT JOIN pap_exclusion_loan_sale paps ON rmt.loan_id = paps.loan_id
LEFT JOIN f_refinance_loans_co rl ON rmt.loan_id = rl.loan_id
LEFT JOIN f_kyc_bureau_income_estimator_co kyc_ie ON rmt.application_id = kyc_ie.application_id
LEFT JOIN f_kyc_bureau_personal_info_co kyc_b on rmt.application_id = kyc_b.application_id
LEFT JOIN f_underwriting_fraud_stage_co udw ON rmt.application_id = udw.application_id
WHERE rmt.loan_id IS NOT NULL       -- Only keeping loans

),

employees_ids AS (

SELECT
    ppd.client_id,
    emp.employee_id AS id_number
FROM d_employees_id_co emp
LEFT JOIN d_prospect_personal_data_co ppd ON emp.employee_id = ppd.id_number
WHERE ppd.client_id IS NOT NULL

),

loan_modifications AS (

SELECT DISTINCT
    loan_id
FROM f_syc_refinancing_instructions_co

  UNION

SELECT DISTINCT
    loan_id
FROM f_syc_condonations_co
WHERE type IN ('CONDONATION_BY_CLIENT')

),

max_dpd_loan AS (

SELECT
  loan_id,
  MAX(days_past_due) AS loan_max_days_past_due
FROM dm_daily_loan_status_co
GROUP BY 1

),

max_dpd_client AS (

SELECT
  client_id,
  MAX(days_past_due) AS client_max_days_past_due
FROM dm_daily_loan_status_co
GROUP BY 1

),

client_loan_calculations AS (

SELECT
    loan_id,
    applicable_rate,
    total_principal_paid,
    total_interest_paid,
    min_payment
FROM d_snc_client_loan_calculations_co

),

client_paid_installments AS (

SELECT DISTINCT
  client_id,
  COALESCE (SUM(paid_installments) OVER( PARTITION BY dmo.client_id ), 0) AS client_total_paid_installments
FROM risk_master_table rmt
LEFT JOIN dm_originations_base dmo ON rmt.loan_id = dmo.loan_id
WHERE 1=1
  AND rmt.loan_id IS NOT NULL
  AND dmo.origination_date >= '2020-04-01'
  AND dmo.origination_date::date <= date_sub(now(), 1)

),

all_originations AS (

SELECT
    application_id,
    loan_id,
    client_id,
    origination_date,
    expected_final_losses,
    COALESCE(lead_gen_fee_rate, 0) AS lead_gen_fee_rate,
    interest_rate,
    FALSE AS is_refinanced_loan,
    guarantee_rate,
    guarantee_provider_with_default AS guarantee_provider
FROM dm_originations_base

UNION

SELECT
    application_id,
    loan_id,
    client_id,
    origination_date,
    NULL AS expected_final_losses,
    0 AS lead_gen_fee_rate,
    effective_annual_rate AS interest_rate,
    TRUE AS is_refinanced_loan,
    guarantee_rate,
    NULL as guarantee_provider
FROM f_refinance_loans_co

),

dm_originations_with_joins AS (

SELECT
    ori.application_id,
    ori.loan_id,
    ori.client_id,
    ori.origination_date,
    COALESCE(ori.expected_final_losses, NULL) AS expected_final_losses,
    if(emp.client_id IS NOT NULL, TRUE, FALSE) AS is_addi_employee,
    if(lmd.loan_id IS NOT NULL OR dlc.custom_has_partial_cancellations, TRUE, FALSE) AS loan_is_modified,
    usr.usury_rate,
    usr.low_balance_usury_rate,
    clc.applicable_rate,
    clc.total_principal_paid,
    clc.total_interest_paid,
    clc.min_payment AS current_installment_amount,
    COALESCE(ori.lead_gen_fee_rate, 0) AS lead_gen_fee_rate,
    ori.interest_rate,
    mdl.loan_max_days_past_due,
    mdc.client_max_days_past_due,
    lo.loan_ownership,
    lo.sale_date,
    rtf.reason_to_filter AS reason_to_filter,
    cpi.client_total_paid_installments,
    COALESCE(ffc.is_fga_claimed, false) AS is_fga_claimed,
    ffc.condonation_date,
    ori.guarantee_provider,
    ori.guarantee_rate,
    COALESCE(dlc.custom_is_totally_cancelled, FALSE) AS is_cancelled,
    dlc.last_total_cancellation_reason AS cancellation_reason,
    COALESCE(dlc.custom_has_partial_cancellations, FALSE) AS has_partial_cancellations,
    lbl.number_of_financial_tradelines,
    lbl.number_of_financial_lbl_tradelines,
    lbl.total_lbl_amount
FROM all_originations ori
LEFT JOIN d_syc_usury_rates_co usr
  ON ori.origination_date BETWEEN usr.start_date AND usr.end_date
LEFT JOIN loan_ownership_co lo ON ori.loan_id = lo.loan_id
LEFT JOIN reason_to_filter_loans rtf ON ori.loan_id = rtf.loan_id
LEFT JOIN employees_ids emp ON ori.client_id = emp.client_id
LEFT JOIN loan_modifications lmd ON ori.loan_id = lmd.loan_id
LEFT JOIN client_loan_calculations clc ON ori.loan_id = clc.loan_id
LEFT JOIN max_dpd_loan mdl ON ori.loan_id = mdl.loan_id
LEFT JOIN max_dpd_client mdc ON ori.client_id = mdc.client_id
LEFT JOIN client_paid_installments cpi ON ori.client_id = cpi.client_id
LEFT JOIN dm_condonations ffc ON ffc.loan_id = ori.loan_id
LEFT JOIN dm_grande_lbl_eligibility_criteria lbl ON ori.loan_id = lbl.loan_id
LEFT JOIN dm_loan_cancellations_co dlc ON ori.loan_id = dlc.loan_id

),

columns_to_display AS (

SELECT
    rmt.application_id,
    rmt.loan_id,
    rmt.addi_pd,
    rmt.ally_is_terminated,
    rmt.ally_name,
    rmt.ally_slug,
    rmt.ally_vertical,
    rmt.ally_region,
    rmt.approved_amount,
    dmo.client_id,
    rmt.credit_score,
    rmt.credit_score_name,
    dmo.applicable_rate AS current_apr,
    dmo.total_principal_paid,
    dmo.total_interest_paid,
    dmo.current_installment_amount,
    dmo.client_max_days_past_due,
    rmt.client_type,
    rmt.days_past_due,
    rmt.estimated_income,
    rmt.credit_check_income_provider,
    dmo.expected_final_losses / rmt.approved_amount AS expected_final_losses_percentage,
    ((rmt.mdf + dmo.lead_gen_fee_rate) * rmt.approved_amount - dmo.expected_final_losses) 
                              / rmt.approved_amount AS expected_net_profit_percentage,
    rmt.expected_maturity_date,
    COALESCE(dmo.guarantee_rate, 0) AS guarantee_rate,
    rmt.first_payment_date,
    rmt.ia_loan AS is_ia_loan,
    dmo.is_addi_employee,
    dmo.is_cancelled,
    dmo.cancellation_reason,
    dmo.has_partial_cancellations,
    rmt.is_fully_paid,
    dmo.is_fga_claimed,
    dmo.condonation_date,
    rmt.is_refinanced_loan,
    rmt.is_returning_client,
    rmt.learning_population,
    rmt.loan_number,
    rmt.mdf AS loan_mdf,
    dmo.lead_gen_fee_rate AS loan_lead_gen_fee_rate,
    dmo.loan_is_modified,
    dmo.loan_max_days_past_due,
    dmo.loan_ownership,
    rmt.low_balance_loan,
    rmt.mob,
    dmo.interest_rate AS origination_apr,
    dmo.origination_date,
    rmt.obligor_age_range,
    rmt.paid_installments,
    dmo.client_total_paid_installments,
    rmt.product,
    rmt.prospect_age_avg,
    dmo.reason_to_filter,
    dmo.sale_date,
    rmt.synthetic_product_category,
    rmt.synthetic_product_subcategory,
    rmt.term,
    rmt.unpaid_principal,
    rmt.tdsr,
    CASE
        WHEN rmt.low_balance_loan = FALSE THEN dmo.usury_rate
        WHEN rmt.low_balance_loan = TRUE THEN dmo.low_balance_usury_rate
        ELSE NULL
    END AS usury_rate,
    dmo.guarantee_provider,
    dmo.number_of_financial_tradelines,
    dmo.number_of_financial_lbl_tradelines,
    dmo.total_lbl_amount,
    CASE
        WHEN dmo.total_lbl_amount <= 5200000
          AND dmo.number_of_financial_tradelines = 0
          THEN true
        WHEN dmo.total_lbl_amount <= 5200000
          AND dmo.number_of_financial_tradelines > 0
          AND dmo.number_of_financial_tradelines = dmo.number_of_financial_lbl_tradelines
          THEN true
        WHEN dmo.number_of_financial_tradelines > 0
          AND dmo.number_of_financial_tradelines != dmo.number_of_financial_lbl_tradelines
          THEN FALSE
        ELSE FALSE
    END AS ltl_flag
FROM risk_master_table rmt
LEFT JOIN dm_originations_with_joins dmo ON rmt.loan_id = dmo.loan_id

),

architect_loan_conditions AS (

SELECT
    rmt.loan_id,
    ARRAY(
        NAMED_STRUCT('column', 'is_cancelled', 'value', CAST(is_cancelled AS STRING)),
        NAMED_STRUCT('column', 'has_partial_cancellations', 'value', CAST(has_partial_cancellations AS STRING)),
        NAMED_STRUCT('column', 'is_fully_paid', 'value', CAST(is_fully_paid AS STRING)),
        NAMED_STRUCT('column', 'ally_is_terminated', 'value', CAST(ally_is_terminated AS STRING)),
        NAMED_STRUCT('column', 'learning_population', 'value', CAST(learning_population AS STRING)),
        NAMED_STRUCT('column', 'days_past_due', 'value', CAST(days_past_due AS STRING)),
        NAMED_STRUCT('column', 'synthetic_product_subcategory', 'value', synthetic_product_subcategory),
        NAMED_STRUCT('column', 'term', 'value', CAST(term AS STRING)),
        NAMED_STRUCT('column', 'client_type', 'value', client_type),
        NAMED_STRUCT('column', 'approved_amount', 'value', CAST(approved_amount AS STRING)),
        NAMED_STRUCT('column', 'loan_is_modified', 'value', CAST(loan_is_modified AS STRING)),
        NAMED_STRUCT('column', 'prospect_age_avg', 'value', CAST(prospect_age_avg AS STRING)),
        NAMED_STRUCT('column', 'client_total_paid_installments', 'value', CAST(client_total_paid_installments AS STRING)),
        NAMED_STRUCT('column', 'loan_number', 'value', CAST(loan_number AS STRING)),
        NAMED_STRUCT('column', 'is_refinanced_loan', 'value', CAST(is_refinanced_loan AS STRING)),
        NAMED_STRUCT('column', 'origination_date', 'value', CAST(origination_date AS TIMESTAMP)),
        NAMED_STRUCT('column', 'guarantee_provider', 'value', CAST(guarantee_provider AS STRING))
    ) AS debug_architect,
    is_cancelled,
    is_fully_paid,
    ally_is_terminated,
    learning_population AS is_learning_population,
    loan_is_modified,
    is_refinanced_loan,
    days_past_due = 0 AS no_days_past_due,
    if(synthetic_product_category IN ('CUPO', 'INTRO'), TRUE, FALSE) AS loan_in_approved_types,
    CASE
        WHEN (synthetic_product_subcategory = 'CUPO 0%' AND term <=3) THEN TRUE
        WHEN (synthetic_product_subcategory IN ('CUPO Flex', 'INTRO') AND term>=3 AND term<=6) THEN TRUE
        WHEN synthetic_product_subcategory NOT IN ('CUPO 0%', 'CUPO Flex', 'INTRO') THEN TRUE
    END AS term_according_to_cupo_product,
    CASE
        WHEN (client_type = 'PROSPECT' AND approved_amount <= 2000000) THEN TRUE
        WHEN (client_type = 'CLIENT' AND approved_amount <= 5000000) THEN TRUE
    END AS approved_amount_in_range,
    CASE
        WHEN (low_balance_loan = TRUE AND COALESCE(applicable_rate, 0) <= low_balance_usury_rate) THEN FALSE
        WHEN (low_balance_loan = FALSE AND COALESCE(applicable_rate, 0) <= usury_rate) THEN FALSE
        ELSE TRUE
    END AS apr_greater_than_usury,
    CASE WHEN prospect_age_avg NOT BETWEEN 18 AND 70 THEN FALSE
    END AS prospect_age_avg_out_of_range,
    CASE
        WHEN reason_to_filter IS NOT NULL THEN TRUE
    END AS loan_has_reason_to_filter,
    CASE
        WHEN loan_number >= 2 AND client_total_paid_installments = 0 THEN TRUE
        ELSE FALSE
    END AS exclude_client_total_paid_installments,
    CASE
        WHEN dmo.origination_date >= current_date() - interval '15 days' THEN TRUE
        ELSE FALSE
    END AS originated_in_last_15_days,
    if(guarantee_provider = "FNG", TRUE, FALSE) AS guarantee_provider_is_fng
FROM risk_master_table rmt
LEFT JOIN dm_originations_with_joins dmo ON rmt.loan_id = dmo.loan_id

),

architect_elegible_loans AS (

SELECT
    loan_id,
    debug_architect,
    CASE
        WHEN (is_cancelled = FALSE
        AND is_fully_paid = FALSE
        AND ally_is_terminated = FALSE
        AND is_learning_population = FALSE
        AND no_days_past_due = TRUE
        AND loan_in_approved_types = TRUE
        AND term_according_to_cupo_product = TRUE
        AND approved_amount_in_range = TRUE
        AND apr_greater_than_usury = FALSE
        AND loan_is_modified IS NOT TRUE
        AND prospect_age_avg_out_of_range IS NOT FALSE
        AND loan_has_reason_to_filter IS NOT TRUE
        AND exclude_client_total_paid_installments IS NOT TRUE
        AND is_refinanced_loan IS NOT TRUE
        AND originated_in_last_15_days = TRUE
        AND guarantee_provider_is_fng = FALSE) THEN TRUE
        ELSE FALSE
    END AS loan_elegible_for_architect

FROM architect_loan_conditions

),

goldman_loan_conditions AS (

SELECT
    rmt.loan_id,
    ARRAY(
        NAMED_STRUCT('column', 'is_cancelled', 'value', CAST(is_cancelled AS STRING)),
        NAMED_STRUCT('column', 'has_partial_cancellations', 'value', CAST(has_partial_cancellations AS STRING)),
        NAMED_STRUCT('column', 'is_fully_paid', 'value', CAST(is_fully_paid AS STRING)),
        NAMED_STRUCT('column', 'days_past_due', 'value', CAST(days_past_due AS STRING)),
        NAMED_STRUCT('column', 'expected_maturity_date', 'value', CAST(expected_maturity_date AS STRING)),
        NAMED_STRUCT('column', 'synthetic_product_category', 'value', synthetic_product_category),
        NAMED_STRUCT('column', 'synthetic_product_subcategory', 'value', synthetic_product_subcategory),
        NAMED_STRUCT('column', 'term', 'value', CAST(term AS STRING)),
        NAMED_STRUCT('column', 'is_addi_employee', 'value', CAST(is_addi_employee AS STRING)),
        NAMED_STRUCT('column', 'learning_population', 'value', CAST(learning_population AS STRING)),
        NAMED_STRUCT('column', 'loan_is_modified', 'value', CAST(loan_is_modified AS STRING)),
        NAMED_STRUCT('column', 'low_balance_loan', 'value', CAST(low_balance_loan AS STRING)),
        NAMED_STRUCT('column', 'applicable_rate', 'value', CAST(applicable_rate AS STRING)),
        NAMED_STRUCT('column', 'usury_rate', 'value', CAST(usury_rate AS STRING)),
        NAMED_STRUCT('column', 'low_balance_usury_rate', 'value', CAST(low_balance_usury_rate AS STRING)),
        NAMED_STRUCT('column', 'expected_final_losses', 'value', CAST(expected_final_losses AS STRING)),
        NAMED_STRUCT('column', 'approved_amount', 'value', CAST(approved_amount AS STRING)),
        NAMED_STRUCT('column', 'expected_final_losses / approved_amount', 'value', CAST(expected_final_losses / approved_amount AS STRING)),
        NAMED_STRUCT('column', '(mdf + lead_gen_fee_rate) * approved_amount - expected_final_losses', 'value', CAST((mdf + lead_gen_fee_rate) * approved_amount - expected_final_losses AS STRING)),
        NAMED_STRUCT('column', 'is_refinanced_loan', 'value', CAST(is_refinanced_loan AS STRING)),
        NAMED_STRUCT('column', 'origination_date', 'value', CAST(origination_date AS TIMESTAMP))
    ) AS debug_goldman,
    is_cancelled,
    is_fully_paid,
    days_past_due <= 0 AS days_past_due_leq_than_0,
    expected_maturity_date::date > current_date() AS loan_has_not_matured,
    is_addi_employee,
    loan_is_modified,
    is_refinanced_loan,
    learning_population AS is_learning_population,
    synthetic_product_category IN ('INTRO', 'CUPO', 'GRANDE') AS loan_in_approved_types,
    CASE
        WHEN synthetic_product_subcategory = 'CUPO 0%' AND ((mdf + lead_gen_fee_rate) * approved_amount - expected_final_losses) < (-0.025 * approved_amount) THEN TRUE
    END AS cupo_0_net_profit_under,
    CASE
        WHEN (synthetic_product_subcategory = 'CUPO 0%' AND term <= 3) THEN TRUE
        WHEN (synthetic_product_subcategory = 'CUPO Flex' AND term <= 6) THEN TRUE
        WHEN (synthetic_product_category = 'INTRO' AND term <= 6) THEN TRUE
        WHEN (synthetic_product_category = 'GRANDE' AND term <= 24) THEN TRUE
    END AS term_according_to_product_goldman,
    CASE
        WHEN (synthetic_product_category = 'GRANDE' AND low_balance_loan = TRUE AND applicable_rate > low_balance_usury_rate) THEN TRUE
        WHEN (synthetic_product_category = 'GRANDE' AND low_balance_loan = FALSE AND applicable_rate > usury_rate) THEN TRUE
    END AS apr_greater_than_usury,
    CASE
        WHEN synthetic_product_category IN ('CUPO', 'INTRO') AND expected_final_losses / approved_amount > 0.075 THEN TRUE
    END AS cupo_expected_losses_geq_range,
    CASE
        WHEN reason_to_filter IS NOT NULL THEN TRUE
    END AS loan_has_reason_to_filter,
    CASE
        WHEN dmo.origination_date >= current_date() - interval '15 days' THEN TRUE
        ELSE FALSE
    END AS originated_in_last_15_days
FROM risk_master_table rmt
LEFT JOIN dm_originations_with_joins dmo ON rmt.loan_id = dmo.loan_id
),

goldman_elegible_loans AS (

SELECT
    loan_id,
    debug_goldman,
    CASE
        WHEN (is_cancelled = FALSE
        AND is_fully_paid = FALSE
        AND days_past_due_leq_than_0 = TRUE
        AND loan_has_not_matured = TRUE
        AND is_addi_employee IS NOT TRUE
        AND is_learning_population IS NOT TRUE
        AND loan_is_modified IS NOT TRUE
        AND loan_in_approved_types = TRUE
        AND cupo_0_net_profit_under IS NOT TRUE
        AND term_according_to_product_goldman = TRUE
        AND apr_greater_than_usury IS NOT TRUE
        AND cupo_expected_losses_geq_range IS NOT TRUE
        AND loan_has_reason_to_filter IS NOT TRUE
        AND is_refinanced_loan IS NOT TRUE
        AND originated_in_last_15_days = TRUE) THEN TRUE
        ELSE FALSE
    END AS loan_elegible_for_goldman

FROM goldman_loan_conditions

),

final_base AS (

SELECT
  ctd.*,
  ael.loan_elegible_for_architect,
  gel.loan_elegible_for_goldman,
  ael.debug_architect,
  gel.debug_goldman
FROM columns_to_display ctd
LEFT JOIN architect_elegible_loans ael ON ctd.loan_id = ael.loan_id
LEFT JOIN goldman_elegible_loans gel ON ctd.loan_id = gel.loan_id
WHERE 1=1
  AND origination_date >= '2020-04-01'
  AND origination_date::date <= date_sub(now(), 1)
)

SELECT * FROM final_base
ORDER BY origination_date DESC;
