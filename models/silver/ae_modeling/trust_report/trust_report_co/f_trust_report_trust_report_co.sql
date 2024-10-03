{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH kordev_toggle_updates AS (
    SELECT
      *
    FROM {{ ref('d_syc_client_migration_segments_co') }}
),

trust_report_trust_report_co AS (
SELECT
    trtr.loan_id,
    trtr.client_id,
    trtr.amount_originated_this_month,
    trtr.bucket,
    trtr.cancellation_reason,
    trtr.cancelled_and_annulled_loan_initial_balance,
    trtr.current_interest_covered_this_month,
    trtr.current_principal_condoned_this_month,
    trtr.current_principal_covered_this_month,
    trtr.days_past_due,
    trtr.first_period_interest_condonation_next_month,
    trtr.first_period_interest_condonation_this_month,
    trtr.full_payment_guarantee_condonation_adjustment,
    trtr.created_at,
    trtr.initial_loan_balance,
    trtr.interest_accrued_for_period_in_next_month,
    trtr.interest_accrued_this_month,
    trtr.interest_overdue_covered_this_month,
    trtr.interest_overdue_into_upb_this_month,
    case when trtr.loan_ownership = 'Goldman' then 'PA_ADDI_GOLDMAN'
         when trtr.loan_ownership = 'Architect' then 'PA_ADDI_ARCHITECT'
         when trtr.loan_ownership = '0' then null
         else trtr.loan_ownership end as loan_ownership,
    trtr.moratory_interest_accrued_this_month,
    trtr.moratory_interest_condoned_this_month,
    trtr.moratory_interest_covered_this_month,
    CAST(trtr.origination_date AS DATE) AS origination_date,
    trtr.payment_redistribution_principal_adjustment,
    trtr.prepayment_benefit_this_month,
    trtr.prepayment_benefit_to_principal_this_month,
    trtr.principal_conciliation,
    trtr.principal_conciliation_ok,
    trtr.principal_from_annulled_payments,
    trtr.principal_from_new_registered_payments,
    trtr.principal_overdue_condoned_this_month,
    trtr.principal_overdue_covered_this_month,
    trtr.report_date,
    trtr.sale_date,
    trtr.sold_amount,
    trtr.total_current_interest_condoned,
    trtr.total_guarantee_covered,
    trtr.total_interest_overdue_condoned,
    trtr.total_principal_condoned,
    trtr.total_principal_covered,
    trtr.unpaid_principal,
    trtr.unpaid_principal_condoned_this_month,
    trtr.unpaid_principal_covered_this_month,
    trtr.usury_rate_adjustment_impact_on_principal,
    trtr.usury_rate_correction,
    trtr.value_to_transfer_to_trust,
    trtr.interest_overdue_condoned_this_month,
    trtr.current_interest_condoned_this_month,
    trtr.total_fga_principal_recovered,
    trtr.total_fga_interest_recovered,
    trtr.fga_principal_recovered_this_month,
    trtr.fga_interest_recovered_this_month,
    'lms' AS loan_tape_source,
    trtr.ingested_at,
    trtr.updated_at
FROM {{ ref('trust_report_trust_report_co') }} trtr
LEFT JOIN kordev_toggle_updates ktu
ON trtr.client_id = ktu.client_id
WHERE (ktu.client_id IS NULL) OR (ktu.client_id IS NOT NULL AND trtr.report_date::DATE < ktu.lms_migrated_at::DATE)
),

kordev_trust_report AS (
SELECT
    tr.loan_id,
    tr.client_id,
    CAST(tr.amount_originated_this_month AS DECIMAL(38,18)) AS amount_originated_this_month,
    tr.bucket,
    tr.cancellation_reason,
    CAST(tr.cancelled_and_annulled_loan_initial_balance AS DECIMAL(38,18)) AS cancelled_and_annulled_loan_initial_balance,
    CAST(tr.current_interest_covered_this_month AS DECIMAL(38,18)) AS current_interest_covered_this_month,
    CAST(tr.current_principal_condoned_this_month AS DECIMAL(38,18)) AS current_principal_condoned_this_month,
    CAST(tr.current_principal_covered_this_month AS DECIMAL(38,18)) AS current_principal_covered_this_month,
    tr.days_past_due,
    CAST(tr.first_period_interest_condonation_next_month AS DECIMAL(38,18)) AS first_period_interest_condonation_next_month,
    CAST(tr.first_period_interest_condonation_this_month AS DECIMAL(38,18)) AS first_period_interest_condonation_this_month,
    CAST(tr.full_payment_guarantee_condonation_adjustment AS DECIMAL(38,18)) AS full_payment_guarantee_condonation_adjustment,
    tr.created_at,
    CAST(tr.initial_loan_balance AS DECIMAL(38,18)) AS initial_loan_balance,
    CAST(tr.interest_accrued_for_period_in_next_month AS DECIMAL(38,18)) AS interest_accrued_for_period_in_next_month,
    CAST(tr.interest_accrued_this_month AS DECIMAL(38,18)) AS interest_accrued_this_month,
    CAST(tr.interest_overdue_covered_this_month AS DECIMAL(38,18)) AS interest_overdue_covered_this_month,
    CAST(tr.interest_overdue_into_upb_this_month AS DECIMAL(38,18)) AS interest_overdue_into_upb_this_month,
    case when tr.loan_ownership = 'Goldman' then 'PA_ADDI_GOLDMAN'
         when tr.loan_ownership = 'Architect' then 'PA_ADDI_ARCHITECT'
         when tr.loan_ownership = '0' then null
         else tr.loan_ownership end as loan_ownership,
    CAST(tr.moratory_interest_accrued_this_month AS DECIMAL(38,18)) AS moratory_interest_accrued_this_month,
    CAST(tr.moratory_interest_condoned_this_month AS DECIMAL(38,18)) AS moratory_interest_condoned_this_month,
    CAST(tr.moratory_interest_covered_this_month AS DECIMAL(38,18)) AS moratory_interest_covered_this_month,
    CAST(tr.origination_date AS DATE) AS origination_date,
    CAST(tr.payment_redistribution_principal_adjustment AS DECIMAL(38,18)) AS payment_redistribution_principal_adjustment,
    CAST(tr.prepayment_benefit_this_month AS DECIMAL(38,18)) AS prepayment_benefit_this_month,
    CAST(tr.prepayment_benefit_to_principal_this_month AS DECIMAL(38,18)) AS prepayment_benefit_to_principal_this_month,
    CAST(tr.principal_conciliation AS DECIMAL(38,18)) AS principal_conciliation,
    CAST(tr.principal_conciliation_ok AS BOOLEAN) AS principal_conciliation_ok,
    CAST(tr.principal_from_annulled_payments AS DECIMAL(38,18)) AS principal_from_annulled_payments,
    CAST(tr.principal_from_new_registered_payments AS DECIMAL(38,18)) AS principal_from_new_registered_payments,
    CAST(tr.principal_overdue_condoned_this_month AS DECIMAL(38,18)) AS principal_overdue_condoned_this_month,
    CAST(tr.principal_overdue_covered_this_month AS DECIMAL(38,18)) AS principal_overdue_covered_this_month,
    tr.report_date,
    CAST(tr.sale_date AS DATE) AS sale_date,
    CAST(tr.sold_amount AS DECIMAL(38,18)) AS sold_amount,
    CAST(tr.total_current_interest_condoned AS DECIMAL(38,18)) AS total_current_interest_condoned,
    CAST(tr.total_guarantee_covered AS DECIMAL(38,18)) AS total_guarantee_covered,
    CAST(tr.total_interest_overdue_condoned AS DECIMAL(38,18)) AS total_interest_overdue_condoned,
    CAST(tr.total_principal_condoned AS DECIMAL(38,18)) AS total_principal_condoned,
    CAST(tr.total_principal_covered AS DECIMAL(38,18)) AS total_principal_covered,
    CAST(tr.unpaid_principal AS DECIMAL(38,18)) AS unpaid_principal,
    CAST(tr.unpaid_principal_condoned_this_month AS DECIMAL(38,18)) AS unpaid_principal_condoned_this_month,
    CAST(tr.unpaid_principal_covered_this_month AS DECIMAL(38,18)) AS unpaid_principal_covered_this_month,
    CAST(tr.usury_rate_adjustment_impact_on_principal AS DECIMAL(38,18)) AS usury_rate_adjustment_impact_on_principal,
    CAST(tr.usury_rate_correction AS DECIMAL(38,18)) AS usury_rate_correction,
    CAST(tr.value_to_transfer_to_trust AS DECIMAL(38,18)) AS value_to_transfer_to_trust,
    CAST(tr.interest_overdue_condoned_this_month AS DECIMAL(38,18)) AS interest_overdue_condoned_this_month,
    CAST(tr.current_interest_condoned_this_month AS DECIMAL(38,18)) AS current_interest_condoned_this_month,
    CAST(tr.total_fga_principal_recovered AS DECIMAL(38,18)) AS total_fga_principal_recovered,
    CAST(tr.total_fga_interest_recovered AS DECIMAL(38,18)) AS total_fga_interest_recovered,
    CAST(tr.fga_principal_recovered_this_month AS DECIMAL(38,18)) AS fga_principal_recovered_this_month,
    CAST(tr.fga_interest_recovered_this_month AS DECIMAL(38,18)) AS fga_interest_recovered_this_month,
    'kordev' AS loan_tape_source,
    tr.ingested_at,
    tr.updated_at
FROM {{ source('servicing', 'trust_report_silver') }} tr
LEFT JOIN kordev_toggle_updates ktu
ON tr.client_id = ktu.client_id
    AND tr.report_date::DATE >= ktu.lms_migrated_at::DATE
WHERE ktu.client_id IS NOT NULL
),

final_table AS (

SELECT *
FROM trust_report_trust_report_co
UNION ALL
SELECT *
FROM kordev_trust_report

)
SELECT *
FROM final_table;
