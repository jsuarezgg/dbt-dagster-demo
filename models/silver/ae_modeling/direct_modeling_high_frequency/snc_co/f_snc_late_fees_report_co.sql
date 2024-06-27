{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--raw.snc_late_fees_report_co
SELECT
    calculation_date,
    client_id,
    loan_id,
    update_date,
    days_past_due,
    unpaid_late_fees AS unpaid_collection_fees,
    late_fees_accrued_this_month AS collection_fees_accrued_this_month,
    late_fees_accrued_without_iva_this_month AS collection_fees_accrued_without_iva_this_month,
    iva_rate_this_month,
    iva_accrued_this_month,
    late_fees_rate_this_month AS collection_fees_rate_this_month,
    principal_overdue_this_month,
    total_late_fees_condoned AS total_collection_fees_condoned,
    total_late_fees_paid AS total_collection_fees_paid
FROM {{ ref('snc_late_fees_report_co') }}