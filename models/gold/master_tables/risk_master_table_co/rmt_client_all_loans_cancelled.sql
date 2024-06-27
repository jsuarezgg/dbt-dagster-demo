{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH prev_loans as (
    SELECT
        base.client_id,
        base.loan_id,
        COUNT(DISTINCT prev.loan_id) AS total_prev_loans
    FROM
        {{ ref('f_originations_bnpl_co') }} base
    INNER JOIN
        {{ ref('f_originations_bnpl_co') }} prev
    ON base.client_id = prev.client_id
        AND prev.origination_date < base.origination_date
    GROUP BY
        base.client_id,
        base.loan_id
),
cancelled_loans AS (
    SELECT
        orig.client_id,
        orig.loan_id,
        COUNT(DISTINCT canc.loan_id) AS cancelled_loans
    FROM
        {{ ref('f_originations_bnpl_co') }} orig
    LEFT JOIN
        {{ ref('f_loan_cancellations_v2_co') }} canc
    ON orig.client_id = canc.client_id
        AND canc.loan_cancellation_order_date <= orig.origination_date
    WHERE
        canc.last_event_name_processed <> 'LoanCancellationOrderAnnulledV2'
    GROUP BY
        orig.client_id,
        orig.loan_id
),
final_table AS (
SELECT
    orig.client_id,
    orig.loan_id,
    prev_loans.total_prev_loans,
    cancelled_loans.cancelled_loans,
    CASE
        WHEN COALESCE(prev_loans.total_prev_loans, 0) = COALESCE(cancelled_loans.cancelled_loans, 0)
                 AND COALESCE(prev_loans.total_prev_loans, 0) > 0
        THEN 'All Prev Loans Cancelled'
        ELSE 'Not All Prev Loans Cancelled'
    END AS loans_cancelled_flag
FROM
    {{ ref('f_originations_bnpl_co') }} AS orig
LEFT JOIN prev_loans ON orig.client_id = prev_loans.client_id AND orig.loan_id = prev_loans.loan_id
LEFT JOIN cancelled_loans ON orig.client_id = cancelled_loans.client_id AND orig.loan_id = cancelled_loans.loan_id
)

select *
from final_table;
