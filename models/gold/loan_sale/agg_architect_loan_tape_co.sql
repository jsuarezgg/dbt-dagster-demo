{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH loan_sale_by_suborder AS (

SELECT
    ls.ally_is_terminated,
    ls.ally_slug,
    ls.ally_vertical,
    ls.approved_amount,
    ls.cancellation_reason,
    ls.client_id,
    ls.client_total_paid_installments,
    ls.current_apr,
    ls.current_installment_amount,
    ls.days_past_due,
    ls.expected_maturity_date,
    ls.first_payment_date,
    ls.guarantee_rate AS fga,
    ls.is_cancelled,
    ls.is_fully_paid,
    ls.is_returning_client,
    ls.learning_population,
    ls.loan_id,
    ls.loan_is_modified,
    ls.loan_lead_gen_fee_rate,
    ls.loan_mdf,
    ls.loan_number,
    ls.origination_apr,
    ls.origination_date,
    ls.prospect_age_avg,
    ls.term,
    ls.total_interest_paid,
    ls.total_principal_paid,
    ls.unpaid_principal,
    ls.usury_rate,
    mk.suborder_attribution_weight_by_total_without_discount_amount * ls.approved_amount AS approved_amount_by_suborder,
    mk.suborder_attribution_weight_by_total_without_discount_amount * ls.current_installment_amount AS current_installment_amount_by_suborder,
    mk.suborder_attribution_weight_by_total_without_discount_amount * ls.unpaid_principal AS unpaid_principal_by_suborder,
    mk.suborder_attribution_weight_by_total_without_discount_amount * ls.total_principal_paid AS total_principal_paid_by_suborder,
    mk.suborder_attribution_weight_by_total_without_discount_amount * ls.total_interest_paid AS total_interest_paid_by_suborder,
    mk.suborder_id AS marketplace_suborder_id,
    mk.suborder_id IS NOT NULL OR ls.ally_slug = 'addi-marketplace' AS is_marketplace,
    mk.suborder_marketplace_purchase_fee,
    mk.suborder_ally_slug AS ally_slug_by_suborder,
    am.vertical_name AS ally_vertical_by_suborder,
    CASE
        WHEN am.ally_state NOT IN ('CREATED', 'PRE_ACTIVE', 'ACTIVE', 'COLD', 'CHURNED') THEN TRUE
        ELSE FALSE
    END AS ally_is_terminated_by_suborder,
    fmt.id_number,
    cc.storage_location,
    lo.sale_date
FROM {{ ref('dm_loan_sale_co') }} ls 
LEFT JOIN {{ ref('dm_originations_marketplace_suborders_co') }} mk ON ls.loan_id = mk.loan_id -- 1:M Join
LEFT JOIN {{ ref('d_ally_management_allies_co') }} am ON mk.suborder_ally_slug = am.ally_slug
LEFT JOIN {{ ref('fraud_master_table_co') }} fmt on ls.loan_id = fmt.loan_id
LEFT JOIN {{ ref('f_client_management_credit_contracts_co') }} cc on ls.loan_id = cc.loan_id
LEFT JOIN {{ ref('loan_ownership_co') }} lo on ls.loan_id = lo.loan_id
WHERE lo.loan_ownership = 'PA_ADDI_ARCHITECT'

)

SELECT
    client_id,
    loan_id,
    from_utc_timestamp(now(), 'America/Bogota')::date AS calculation_date,
    ROUND(if(is_marketplace, unpaid_principal_by_suborder, unpaid_principal), 2) AS unpaid_principal,
    if(is_marketplace, approved_amount_by_suborder, approved_amount) AS approved_amount,
    current_apr,
    ROUND(origination_apr, 4) AS origination_apr,
    if(is_marketplace, ally_vertical_by_suborder, ally_vertical) AS ally_vertical,
    term,
    days_past_due,
    origination_date,
    expected_maturity_date,
    ROUND(if(is_marketplace, current_installment_amount_by_suborder, current_installment_amount), 2) AS current_installment_amount,
    cancellation_reason,
    if(is_marketplace, ally_slug_by_suborder, ally_slug) AS ally_name,
    CASE
        WHEN term >=3 AND term <=6 AND origination_apr > 0 THEN 'FLEX_CO_ARCHITECT'
        ELSE 'PAGO_CO_ARCHITECT'
    END AS product,
    if(is_marketplace, suborder_marketplace_purchase_fee, loan_mdf) AS ally_mdf,
    prospect_age_avg,
    is_cancelled,
    is_fully_paid,
    if(is_marketplace, ally_is_terminated_by_suborder, ally_is_terminated) AS ally_is_terminated,
    is_returning_client AS returning_client,
    CASE
        WHEN loan_number >= 2 AND client_total_paid_installments = 0 THEN TRUE
        ELSE FALSE
    END AS client_no_payments,
    if(is_marketplace, total_principal_paid_by_suborder, total_principal_paid) AS total_principal_paid,
    if(is_marketplace, total_interest_paid_by_suborder, total_interest_paid) AS total_interest_paid,
    usury_rate,
    learning_population,
    fga,
    loan_lead_gen_fee_rate AS affiliate_fee,
    loan_is_modified AS loan_modifications,
    sale_date,
    storage_location AS url_contract,
    id_number,
    first_payment_date,
    marketplace_suborder_id,
    is_marketplace
FROM loan_sale_by_suborder
ORDER BY sale_date DESC, loan_id DESC;
