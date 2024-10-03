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

kordev_daily_report_raw_last_calculation_date AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY external_loan_id ORDER BY calculation_date DESC) AS rn
    FROM {{ source('servicing', 'kordev_daily_report_daily_raw') }}
    QUALIFY rn = 1
),

snc_charge_off_report_co AS (
SELECT
    scor.loan_id,
    scor.client_id,
    scor.product_type,
    scor.threshold_days,
    scor.last_days_past_due,
    scor.max_days_past_due,
    scor.calculation_date,
    scor.origination_date,
    scor.first_due_date,
    scor.charge_off_date,
    scor.is_charge_off,
    scor.charge_off_upb,
    scor.charge_off_total,
    scor.recovery_upb,
    scor.recovery_total,
    scor.recovery_fga,
    scor.refinanced,
    scor.condonation_upb_apart_from_fga,
    scor.condonation_total_apart_from_fga,
    scor.created_at,
    scor.days_past_due_at_chargeoff,
    'lms' AS loan_tape_source,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM {{ ref('snc_charge_off_report_co') }} scor
LEFT JOIN kordev_toggle_updates ktu
ON scor.client_id = ktu.client_id
WHERE ktu.client_id IS NULL
),

kordev_charge_off_report_co AS (
SELECT DISTINCT
    external_loan_id AS loan_id,
    external_borrower_id AS client_id,
    CAST(NULL AS STRING) AS product_type,
    120 AS threshold_days,
    0 AS last_days_past_due,
    days_past_due,
    CAST(from_utc_timestamp(calculation_date, 'America/Bogota') AS DATE) AS calculation_date,
    CAST(from_utc_timestamp(origination_date, 'America/Bogota') AS DATE) AS origination_date,
    CAST(NULL AS STRING) AS first_due_date,
    CAST(from_utc_timestamp(charged_off_date, 'America/Bogota') AS DATE) AS charged_off_date,
    CASE WHEN charged_off_date IS NOT NULL THEN true
                                           ELSE false END AS is_charge_off,
    0 AS charge_off_upb,
    0 AS charge_off_total,
    0 AS recovery_upb,
    0 AS recovery_total,
    0 AS recovery_fga,
    CAST(NULL AS BOOLEAN) AS refinanced,
    0 AS condonation_upb_apart_from_fga,
    0 AS condonation_total_apart_from_fga,
    CAST(from_utc_timestamp(calculation_date, 'America/Bogota') AS DATE) AS created_at,
    0 AS days_past_due_at_chargeoff,
    'kordev' AS loan_tape_source,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM kordev_daily_report_raw_last_calculation_date kdrr
INNER JOIN kordev_toggle_updates ktu
ON kdrr.external_borrower_id = ktu.client_id
),

final_table AS (

SELECT *
FROM snc_charge_off_report_co
UNION ALL
SELECT *
FROM kordev_charge_off_report_co

)
SELECT *
FROM final_table;

