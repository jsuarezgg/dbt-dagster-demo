{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.syc_loan_status_br
SELECT
    -- DIRECT MODELING FIELDS
    calculation_date,
    loan_id,
    client_id,
    accrued_interest_this_month,
    applicable_rate,
    condoned_interest_this_month,
    current_installment_amount,
    current_interest_covered_this_month,
    current_interest_covered_this_period,
    current_interest_due_this_period,
    current_principal_covered_this_month,
    current_principal_due_this_period,
    days_past_due,
    full_payment,
    initial_installment_amount,
    interest_condoned_in_first_period,
    interest_on_overdue_principal,
    interest_on_overdue_principal_covered_this_month,
    interest_on_overdue_principal_covered_this_period,
    interest_overdue,
    interest_overdue_covered_this_month,
    interest_overdue_covered_this_period,
    ipmt,
    is_fully_paid,
    min_payment,
    months_on_books,
    paid_installments,
    payday,
    period_prepayment_benefit,
    ppmt,
    principal_overdue,
    principal_overdue_covered_this_month,
    principal_overdue_covered_this_period,
    total_current_interest_condoned,
    total_current_principal_condoned,
    total_interest_overdue_condoned,
    total_interest_paid,
    total_moratory_interest_condoned,
    total_payment_applied,
    total_prepayment_benefit,
    total_principal_overdue_condoned,
    total_principal_paid,
    total_unpaid_principal_condoned,
    unpaid_interest,
    unpaid_principal,
    guarantee_overdue,
    current_guarantee_due_this_period,
    unpaid_guarantee,
    total_guarantee_paid,
    guarantee_covered_this_period,
    guarantee_covered_this_month,
    total_guarantee_condoned,
    vintage,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
from {{ source('raw', 'syc_loan_status_br') }}