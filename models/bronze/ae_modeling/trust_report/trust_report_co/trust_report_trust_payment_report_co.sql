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
  CAST(client_id AS string) AS client_id,
  CAST(date AS date) AS m_vintage,
  CAST(payment_id AS string) AS payment_id,
  CAST(ownership AS string) AS ownership,
  CAST(channel AS string) AS channel,
  CAST(delinquency_iof AS decimal(38,18)) AS delinquency_iof,
  CAST(moratory_interest AS decimal(38,18)) AS moratory_interest,
  CAST(interest_overdue AS decimal(38,18)) AS interest_overdue,
  CAST(principal_overdue AS decimal(38,18)) AS principal_overdue,
  CAST(late_fees AS decimal(38,18)) AS late_fees,
  CAST(guarantee_overdue AS decimal(38,18)) AS guarantee_overdue,
  CAST(current_interest AS decimal(38,18)) AS current_interest,
  CAST(current_principal AS decimal(38,18)) AS current_principal,
  CAST(current_guarantee AS decimal(38,18)) AS current_guarantee,
  CAST(unpaid_guarantee AS decimal(38,18)) AS unpaid_guarantee,
  CAST(prepayment_benefit AS decimal(38,18)) AS prepayment_benefit,
  CAST(unpaid_principal AS decimal(38,18)) AS unpaid_principal,
  CAST(prepayment_benefit_to_principal AS decimal(38,18)) AS prepayment_benefit_to_principal,
  CAST(prepayment_benefit_to_guarantee AS decimal(38,18)) AS prepayment_benefit_to_guarantee,
  CAST(loan_id AS string) AS loan_id,
  CAST(leftover AS decimal(38,18)) AS leftover,
  CAST(total_payment AS decimal(38,18)) AS total_payment,
  CAST(report_date AS date) AS report_date,
  CAST(created_at AS timestamp) AS created_at,
  NOW() AS ingested_at,
  to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM {{ source('raw', 'trust_report_trust_payment_report_co')}}