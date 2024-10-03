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
    SELECT *
    FROM {{ ref('d_syc_client_migration_segments_co') }}
),

snc_payments_report AS (
--bronze.snc_payments_report_co
SELECT DISTINCT
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    spr.index,
    spr.client_id,
    spr.delinquency_iof,
    spr.moratory_interest,
    spr.interest_overdue,
    spr.principal_overdue,
    spr.guarantee_overdue,
    spr.current_interest,
    spr.current_principal,
    spr.current_guarantee,
    spr.unpaid_guarantee,
    spr.unpaid_principal,
    spr.prepayment_benefit,
    spr.date,
    spr.payment_id,
    spr.ownership,
    spr.channel,
    spr.loan_id,
    spr.leftover,
    spr.late_fees AS collection_fees,
    spr.total_payment,
    spr.created_at,
    'lms' as loan_tape_source,
    spr.ingested_at,
    spr.updated_at

-- DBT SOURCE REFERENCE
FROM {{ ref('snc_payments_report_co') }} spr
LEFT JOIN kordev_toggle_updates ktu
ON spr.client_id = ktu.client_id
WHERE ktu.client_id is null
),

kordev_payments_report_co AS (
SELECT DISTINCT
    uuid() as index,
    prs.client_id,
    prs.delinquency_iof,
    prs.moratory_interest,
    prs.interest_overdue,
    prs.principal_overdue,
    prs.guarantee_overdue,
    prs.current_interest,
    prs.current_principal,
    prs.current_guarantee,
    prs.unpaid_guarantee,
    prs.unpaid_principal,
    prs.prepayment_benefit,
    CAST(prs.date AS date) as date,
    prs.payment_id,
    prs.ownership,
    prs.channel,
    prs.loan_id,
    prs.leftover,
    CAST(prs.unpaid_collection_fees AS decimal(38,18)) AS collection_fees,
    CAST(prs.totalpayment AS decimal(38,18)) as total_payment,
    prs.created_at,
    'kordev' as loan_tape_source,
    NOW()::date as ingested_at,
    NOW() as updated_at
-- DBT SOURCE REFERENCE
FROM {{ source('servicing', 'payment_report_silver')}} prs
INNER JOIN kordev_toggle_updates ktu
ON prs.client_id = ktu.client_id
),

final_table AS (

SELECT *
FROM snc_payments_report
UNION ALL
SELECT *
FROM kordev_payments_report_co

)
SELECT *
FROM final_table;
