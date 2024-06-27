{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

SELECT
    loan_id,
    min(calculation_date) AS fully_paid_date
FROM {{ source('gold', 'dm_daily_loan_status_co') }}
    WHERE is_fully_paid IS TRUE
    AND round(total_current_principal_condoned) = 0
    AND round(total_unpaid_principal_condoned) = 0
    AND round(total_principal_overdue_condoned) = 0
    AND round(total_interest_overdue_condoned) = 0
    AND round(total_moratory_interest_condoned) = 0
    AND round(total_guarantee_condoned) = 0
    AND round(total_current_interest_condoned) = 0
    AND round(approved_amount) - round(total_principal_paid) <= 1
GROUP BY 1