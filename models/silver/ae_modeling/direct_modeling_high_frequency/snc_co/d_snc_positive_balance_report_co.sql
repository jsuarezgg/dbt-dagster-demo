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

snc_positive_balance_report_co AS (
SELECT
    -- DIRECT MODELING FIELDS
    spbr.calculation_date,
    spbr.client_id,
    spbr.positive_balance,
    spbr.balance_before_payment,
    spbr.update_date,
    spbr.payments,
    spbr.directed_payment,
    spbr.condonations,
    'lms' AS loan_tape_source,
    -- MANDATORY FIELDS
    spbr.ingested_at,
    spbr.updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('snc_positive_balance_report_co') }} spbr
LEFT JOIN kordev_toggle_updates ktu
ON spbr.client_id = ktu.client_id
WHERE ktu.client_id IS NULL
),

kordev_positive_balance_report_co AS (
SELECT
    kdrr.calculation_date,
    kdrr.external_borrower_id AS client_id,
    MAX(kdrr.positive_balance) AS positive_balance,
    0 AS balance_before_payment,
    CAST(from_utc_timestamp(kdrr.calculation_date, 'America/Bogota') AS DATE) AS update_date,
    '[]' AS payments,
    '{}' AS directed_payment,
    '[]' AS condonations,
    'kordev' as loan_tape_source,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM {{ source('servicing', 'kordev_daily_report_daily_raw') }} kdrr
INNER JOIN kordev_toggle_updates ktu
ON kdrr.external_borrower_id = ktu.client_id
GROUP BY 1, 2
),

final_table AS (

SELECT *
FROM snc_positive_balance_report_co
UNION ALL
SELECT *
FROM kordev_positive_balance_report_co

)
SELECT *
FROM final_table;
