{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

SELECT
channel,
client_id,
created_at,
current_interest,
current_principal,
current_guarantee,
m_vintage,
delinquency_iof,
guarantee_overdue,
interest_overdue,
late_fees AS unpaid_collection_fees,
leftover,
loan_id,
moratory_interest,
ownership,
payment_id,
prepayment_benefit,
prepayment_benefit_to_guarantee,
prepayment_benefit_to_principal,
principal_overdue,
report_date,
total_payment,
unpaid_guarantee,
unpaid_principal,
NOW() AS ingested_at,
to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM {{ ref('trust_report_trust_payment_report_co')}}