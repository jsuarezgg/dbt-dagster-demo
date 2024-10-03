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
)
SELECT
  tpr.channel,
  tpr.client_id,
  tpr.created_at,
  tpr.current_interest,
  tpr.current_principal,
  tpr.current_guarantee,
  tpr.m_vintage,
  tpr.delinquency_iof,
  tpr.guarantee_overdue,
  tpr.interest_overdue,
  tpr.late_fees AS unpaid_collection_fees,
  tpr.leftover,
  tpr.loan_id,
  tpr.moratory_interest,
  tpr.ownership,
  tpr.payment_id,
  tpr.prepayment_benefit,
  tpr.prepayment_benefit_to_guarantee,
  tpr.prepayment_benefit_to_principal,
  tpr.principal_overdue,
  tpr.report_date,
  tpr.total_payment,
  tpr.unpaid_guarantee,
  tpr.unpaid_principal,
  'lms' as loan_tape_source,
  NOW() AS ingested_at,
  to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM {{ ref('trust_report_trust_payment_report_co')}} tpr
LEFT JOIN kordev_toggle_updates ktu 
ON tpr.client_id = ktu.client_id
WHERE (ktu.client_id IS NULL) OR (ktu.client_id IS NOT NULL AND tpr.report_date::DATE < date_trunc('month', ktu.lms_migrated_at))
UNION ALL
SELECT
  prs.channel,
  prs.client_id,
  CAST(prs.created_at AS TIMESTAMP) AS created_at,
  CAST(prs.current_interest AS DECIMAL(38,18)) AS current_interest,
  CAST(prs.current_principal AS DECIMAL(38,18)) AS current_principal,
  CAST(prs.current_guarantee AS DECIMAL(38,18)) AS current_guarantee,
  CAST(prs.date AS DATE) AS m_vintage,
  CAST(prs.delinquency_iof AS DECIMAL(38,18)) AS delinquency_iof,
  CAST(prs.guarantee_overdue AS DECIMAL(38,18)) AS guarantee_overdue,
  CAST(prs.interest_overdue AS DECIMAL(38,18)) AS interest_overdue,
  CAST(prs.unpaid_collection_fees AS DECIMAL(38,18)) AS unpaid_collection_fees,
  CAST(prs.leftover AS DECIMAL(38,18)) AS leftover,
  prs.loan_id,
  CAST(prs.moratory_interest AS DECIMAL(38,18)) AS moratory_interest,
  prs.ownership,
  prs.payment_id,
  CAST(prs.prepayment_benefit AS DECIMAL(38,18)) AS prepayment_benefit,
  CAST(prs.prepayment_benefit_to_guarantee AS DECIMAL(38,18)) AS prepayment_benefit_to_guarantee,
  CAST(prs.prepayment_benefit_to_principal AS DECIMAL(38,18)) AS prepayment_benefit_to_principal,
  CAST(prs.principal_overdue AS DECIMAL(38,18)) AS principal_overdue,
  LAST_DAY(CAST(prs.date AS DATE)) AS report_date,
  CAST(prs.totalpayment AS DECIMAL(38,18)) AS total_payment,
  CAST(prs.unpaid_guarantee AS DECIMAL(38,18)) AS unpaid_guarantee,
  CAST(prs.unpaid_principal AS DECIMAL(38,18)) AS unpaid_principal,
  'kordev' as loan_tape_source,
  NOW() AS ingested_at,
  to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM {{ source('servicing', 'payment_report_silver') }} prs
LEFT JOIN kordev_toggle_updates ktu
ON prs.client_id = ktu.client_id
    AND prs.date::DATE >= date_trunc('month', ktu.lms_migrated_at)
WHERE ktu.client_id IS NOT NULL
AND prs.date::DATE <= (DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '1 day')::DATE