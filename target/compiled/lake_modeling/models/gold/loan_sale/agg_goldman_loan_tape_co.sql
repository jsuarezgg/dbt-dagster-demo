

WITH loan_sale_by_suborder AS (

SELECT
    am.ally_name AS ally_name_by_suborder,
    am.vertical_name AS ally_vertical_by_suborder,
    fmt.id_number,
    lo.sale_date,
    ls.addi_pd,
    ls.ally_name,
    ls.ally_region,
    ls.ally_vertical,
    ls.approved_amount,
    ls.cancellation_reason,
    ls.client_id,
    ls.client_max_days_past_due,
    ls.credit_check_income_provider,
    ls.credit_score_name,
    ls.credit_score,
    ls.current_apr,
    ls.current_installment_amount,
    ls.days_past_due,
    ls.estimated_income,
    ls.expected_final_losses_percentage,
    ls.expected_maturity_date,
    ls.expected_net_profit_percentage,
    ls.first_payment_date,
    ls.guarantee_provider,
    ls.is_addi_employee,
    ls.is_cancelled,
    ls.is_fga_claimed,
    ls.is_fully_paid,
    ls.is_returning_client,
    ls.loan_id,
    ls.loan_is_modified,
    ls.loan_lead_gen_fee_rate,
    ls.loan_max_days_past_due,
    ls.loan_mdf,
    ls.low_balance_loan,
    ls.ltl_flag,
    ls.mob,
    ls.origination_apr,
    ls.origination_date,
    ls.prospect_age_avg,
    ls.synthetic_product_category,
    ls.tdsr,
    ls.term,
    ls.unpaid_principal,
    ls.usury_rate,
    ls.ally_vertical,
    mk.suborder_id AS marketplace_suborder_id,
    mk.suborder_id IS NOT NULL OR ls.ally_slug = 'addi-marketplace' AS is_marketplace,
    mk.suborder_marketplace_purchase_fee,
    mk.suborder_attribution_weight_by_total_without_discount_amount,
    mk.suborder_attribution_weight_by_total_without_discount_amount * ls.approved_amount AS approved_amount_by_suborder,
    mk.suborder_attribution_weight_by_total_without_discount_amount * ls.current_installment_amount AS current_installment_amount_by_suborder,
    mk.suborder_attribution_weight_by_total_without_discount_amount * unpaid_principal AS unpaid_principal_by_suborder
FROM gold.dm_loan_sale_co ls 
LEFT JOIN gold.dm_originations_marketplace_suborders_co mk ON ls.loan_id = mk.loan_id -- 1:M Join
LEFT JOIN silver.d_ally_management_allies_co am ON mk.suborder_ally_slug = am.ally_slug
LEFT JOIN gold.fraud_master_table_co fmt on ls.loan_id = fmt.loan_id
LEFT JOIN gold.loan_ownership_co lo on ls.loan_id = lo.loan_id
WHERE lo.loan_ownership = 'PA_ADDI_GOLDMAN'

)

SELECT
    loan_id,
    marketplace_suborder_id,
    CASE
    WHEN synthetic_product_category IN ('INTRO', 'CUPO') THEN 'ADDICupo'
    WHEN synthetic_product_category = 'GRANDE' THEN 'ADDIGrande'
    END AS product,--LOGIC BY MANUEL BANUS AND ACCEPTED BY JENN
    credit_score,
    credit_score_name AS credit_score_product,
    tdsr,
    if(is_marketplace, suborder_marketplace_purchase_fee, loan_mdf) AS merchant_discount_fee,
    estimated_income,
    credit_check_income_provider,
    ally_region AS region,
    addi_pd,
    loan_is_modified AS foreberance,
    is_returning_client AS returning_client,
    loan_max_days_past_due AS max_dpd,
    CASE
        WHEN (synthetic_product_category = 'GRANDE' AND low_balance_loan IS true) THEN 'ADDIGrande Low Balance Loan'
        WHEN (synthetic_product_category = 'GRANDE' AND origination_apr::double = 0) THEN 'ADDIGrande 0% Receivable'
        WHEN synthetic_product_category = 'GRANDE' THEN 'ADDIGrande Core Receivable' -- (No es LBL, no es O%)`
        WHEN ((synthetic_product_category = 'INTRO' OR synthetic_product_category = 'CUPO') AND origination_apr::double =0) THEN 'ADDICupo 0% Receivable'
        WHEN (synthetic_product_category = 'INTRO' OR synthetic_product_category = 'CUPO') THEN 'ADDICupo Receivable' --(No es 0%)
    END AS product_type, --LOGIC BY MANUEL BANUS AND ACCEPTED BY JENN
    client_id,
    current_apr,
    origination_apr,
    low_balance_loan,
    usury_rate AS current_usury_rate,
    expected_maturity_date,
    if(is_marketplace, current_installment_amount_by_suborder, current_installment_amount) AS current_installment_amount,
    if(is_marketplace, ally_name_by_suborder, ally_name) AS ally_name,
    if(is_marketplace, ally_vertical_by_suborder, ally_vertical) AS ally_vertical,
    is_marketplace,
    is_fully_paid,
    is_cancelled,
    client_max_days_past_due AS client_max_dpd,
    prospect_age_avg AS obligor_age_range,
    if(is_marketplace, approved_amount_by_suborder, approved_amount) AS approved_amount,
    cancellation_reason,
    first_payment_date,
    mob,
    origination_date,
    term,
    if(is_marketplace, unpaid_principal_by_suborder, unpaid_principal) AS unpaid_principal,
    days_past_due,
    CASE
    WHEN synthetic_product_category = 'GRANDE' AND origination_date < '2023-07-01' THEN addi_pd
    ELSE expected_final_losses_percentage
    END AS expected_final_losses_percentage,
    expected_net_profit_percentage,
    loan_lead_gen_fee_rate,
    is_addi_employee,
    id_number,
    sale_date,
    is_fga_claimed,
    guarantee_provider,
    ltl_flag
FROM loan_sale_by_suborder
ORDER BY sale_date DESC, loan_id DESC;