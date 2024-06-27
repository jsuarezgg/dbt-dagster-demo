{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

-- STEP 1: Getting condonations from: domain syc table condonations from domain table + events modeled for loan refinance
WITH syc_condonations AS (
  SELECT
    loan_id,
    bucket AS condonation_bucket,
    reason AS condonation_reason,
    type AS condonation_type,
    date AS condonation_date --UTC datetime
  FROM {{ ref('f_syc_condonations_co') }}
)
,
refinance_condonations AS (
    SELECT
        -- Loan Refinance Condonations, capturing post-refinance condonation date fixes
        rc.loan_id,
        rc.condonation_bucket,
        rc.condonation_reason, --condonation_reason: Successful refinance done by the client
        rc.condonation_type, --condonation_type:CONDONATION_BY_ORIGINATIONS
        TO_DATE(FROM_UTC_TIMESTAMP(COALESCE(sc.condonation_date,rc.condonation_date) , 'America/Bogota')) AS condonation_date
    FROM {{ ref('f_refinance_condonations_co') }} AS rc
    LEFT JOIN(
        --Only last state in syc refinance loan, used to capture some post refinance-condonation fixes to amount and date
        SELECT loan_id, condonation_date
        FROM syc_condonations QUALIFY ROW_NUMBER() OVER (PARTITION BY loan_id ORDER BY condonation_date DESC) = 1
        ) AS sc ON rc.loan_id = sc.loan_id
)
,
joint_condonations AS (
    (
    SELECT
      loan_id,
      condonation_bucket,
      condonation_reason,
      condonation_type,
      TO_DATE(FROM_UTC_TIMESTAMP(condonation_date, 'America/Bogota')) AS condonation_date
    FROM syc_condonations
    -- Ignore data from loans condonations that are pre-processed in refinance_condonations CTE
    WHERE loan_id NOT IN (SELECT loan_id FROM refinance_condonations)
    )
    UNION ALL
    (
    -- ADD data from loans condonations that are pre-processed in refinance_condonations CTE
    SELECT
       loan_id,
       condonation_bucket,
       condonation_reason, --condonation_reason: Successful refinance done by the client
       condonation_type, -- condonation_type:CONDONATION_BY_ORIGINATIONS
       condonation_date
    FROM refinance_condonations
    )
)
,
raw_condonations AS (
    SELECT
        loan_id,
        condonation_bucket,
        -- condonation_reason AS condonation_reason_original, --debug
        -- condonation_type AS condonation_reason_original, --debug
        CASE
        WHEN condonation_reason IN ('FGA CLAIM', 'FGA_CLAIM') THEN 'FGA_CLAIM'
        WHEN   condonation_reason ILIKE '%COLLECTION%'
            OR condonation_reason ILIKE '%INCONSISTENCIA MONTO ORIGINADO%'
            OR condonation_reason ILIKE '%internal_control%' THEN 'COLLECTIONS'
        WHEN condonation_reason ilike '%CONDONACION GENERAL (COLLECTIONS)%' THEN 'COLLECTIONS'
        WHEN condonation_reason = 'Successful refinance done by the client' OR
             condonation_type='CONDONATION_BY_ORIGINATIONS' THEN 'LOAN_REFINANCE' --Updated, previously 'LOAN_RECAST'
        ELSE 'OTHER_REASONS'
        END AS condonation_reason,
        condonation_date
    FROM joint_condonations
)
,
-- STEP 2: Cleaning duplicated rows from manual condonations
clean_raw_condonations AS (
  SELECT
    DISTINCT *
  FROM raw_condonations
)
,
-- STEP 3: Removing weird behaviours about decreasing condonation amounts in daily loan status (when only one record is affected; edge cases won't be fixed)
daily_loan_status_co_step_1 AS (
  SELECT
    loan_id,
    calculation_date,
    CASE
      WHEN total_moratory_interest_condoned > LEAD(total_moratory_interest_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date) THEN LEAD(total_moratory_interest_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date)
      ELSE total_moratory_interest_condoned
      END AS total_moratory_interest_condoned,
    CASE
      WHEN total_interest_overdue_condoned > LEAD(total_interest_overdue_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date) THEN LEAD(total_interest_overdue_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date)
      ELSE total_interest_overdue_condoned
      END AS total_interest_overdue_condoned,
    CASE
      WHEN total_principal_overdue_condoned > LEAD(total_principal_overdue_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date) THEN LEAD(total_principal_overdue_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date)
      ELSE total_principal_overdue_condoned
      END AS total_principal_overdue_condoned,
    CASE
      WHEN total_current_interest_condoned > LEAD(total_current_interest_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date) THEN LEAD(total_current_interest_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date)
      ELSE total_current_interest_condoned
      END AS total_current_interest_condoned,
    CASE
      WHEN total_current_principal_condoned > LEAD(total_current_principal_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date) THEN LEAD(total_current_principal_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date)
      ELSE total_current_principal_condoned
      END AS total_current_principal_condoned,
    CASE
      WHEN total_unpaid_principal_condoned > LEAD(total_unpaid_principal_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date) THEN LEAD(total_unpaid_principal_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date)
      ELSE total_unpaid_principal_condoned
      END AS total_unpaid_principal_condoned,
    CASE
      WHEN total_guarantee_condoned > LEAD(total_guarantee_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date) THEN LEAD(total_guarantee_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date)
      ELSE total_guarantee_condoned
      END AS total_guarantee_condoned
  FROM {{ source('gold', 'dm_daily_loan_status_co') }}
 --WHERE loan_id = 'cce04715-08d5-45f1-9035-b8c1952d19db'
)
,
-- STEP 4: Gathering only records whose calculation_date displays a change in any condoned amounts
daily_loan_status_co_step_2 AS (
  SELECT
    loan_id,
    calculation_date,
    COALESCE(CASE WHEN total_moratory_interest_condoned > LAG(total_moratory_interest_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date) THEN total_moratory_interest_condoned END, 0) AS total_moratory_interest_condoned,
    COALESCE(CASE WHEN total_interest_overdue_condoned > LAG(total_interest_overdue_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date) THEN total_interest_overdue_condoned END, 0) AS total_interest_overdue_condoned,
    COALESCE(CASE WHEN total_principal_overdue_condoned > LAG(total_principal_overdue_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date) THEN total_principal_overdue_condoned END, 0) AS total_principal_overdue_condoned,
    COALESCE(CASE WHEN total_current_interest_condoned > LAG(total_current_interest_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date) THEN total_current_interest_condoned END, 0) AS total_current_interest_condoned,
    COALESCE(CASE WHEN total_current_principal_condoned > LAG(total_current_principal_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date) THEN total_current_principal_condoned END, 0) AS total_current_principal_condoned,
    COALESCE(CASE WHEN total_unpaid_principal_condoned > LAG(total_unpaid_principal_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date) THEN total_unpaid_principal_condoned END, 0) AS total_unpaid_principal_condoned,
    COALESCE(CASE WHEN total_guarantee_condoned > LAG(total_guarantee_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date) THEN total_guarantee_condoned END, 0) AS total_guarantee_condoned
  FROM daily_loan_status_co_step_1
)
,
-- STEP 5: INFERRING CONDONDED AMOUNT BASED ON THE PREVIOUS CALCULATION DATE ROW
daily_loan_status_co_step_3 AS (
  SELECT
    loan_id,
    calculation_date,
    CASE
      WHEN total_moratory_interest_condoned > LAG(total_moratory_interest_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date) THEN total_moratory_interest_condoned - LAG(total_moratory_interest_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date)
      ELSE total_moratory_interest_condoned
      END AS total_moratory_interest_condoned,
    CASE
      WHEN total_interest_overdue_condoned > LAG(total_interest_overdue_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date) THEN total_interest_overdue_condoned - LAG(total_interest_overdue_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date)
      ELSE total_interest_overdue_condoned
      END AS total_interest_overdue_condoned,
    CASE
      WHEN total_principal_overdue_condoned > LAG(total_principal_overdue_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date) THEN total_principal_overdue_condoned - LAG(total_principal_overdue_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date)
      ELSE total_principal_overdue_condoned
      END AS total_principal_overdue_condoned,
    CASE
      WHEN total_current_interest_condoned > LAG(total_current_interest_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date) THEN total_current_interest_condoned - LAG(total_current_interest_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date)
      ELSE total_current_interest_condoned
      END AS total_current_interest_condoned,
    CASE
      WHEN total_current_principal_condoned > LAG(total_current_principal_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date) THEN total_current_principal_condoned - LAG(total_current_principal_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date)
      ELSE total_current_principal_condoned
      END AS total_current_principal_condoned,
    CASE
      WHEN total_unpaid_principal_condoned > LAG(total_unpaid_principal_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date) THEN total_unpaid_principal_condoned - LAG(total_unpaid_principal_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date)
      ELSE total_unpaid_principal_condoned
      END AS total_unpaid_principal_condoned,
    CASE
      WHEN total_guarantee_condoned > LAG(total_guarantee_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date) THEN total_guarantee_condoned - LAG(total_guarantee_condoned) OVER(PARTITION BY loan_id ORDER BY calculation_date)
      ELSE total_guarantee_condoned
      END AS total_guarantee_condoned
  FROM daily_loan_status_co_step_2
  WHERE (total_moratory_interest_condoned > 0
      OR total_interest_overdue_condoned > 0
      OR total_principal_overdue_condoned > 0
      OR total_current_interest_condoned > 0
      OR total_current_principal_condoned > 0
      OR total_unpaid_principal_condoned > 0
      OR total_guarantee_condoned > 0)
  ORDER BY loan_id , calculation_date
)
,
-- STEP 6: Joining daily loan status condoned amounts with the syc table, for collecting the condonation reason and date
full_base AS (
  SELECT
    crc.loan_id AS loan_id_condonations,
    crc.condonation_date AS condonation_date_original,
    dls3.calculation_date AS condonation_date,
    COALESCE(crc.condonation_reason, 'OTHER_REASONS') AS condonation_reason,
    dls3.*
  FROM daily_loan_status_co_step_3 dls3
  LEFT JOIN clean_raw_condonations crc  ON dls3.loan_id = crc.loan_id AND dls3.calculation_date >= crc.condonation_date
)
,
-- STEP 7: Computing the nearest condonation reason based on the calculation & condonation dates distances
full_base_2 AS (
  SELECT
    loan_id,
    condonation_date,
    condonation_date_original,
    condonation_reason,
    COALESCE(total_moratory_interest_condoned, 0) AS total_moratory_interest_condoned,
    COALESCE(total_interest_overdue_condoned, 0) AS total_interest_overdue_condoned,
    COALESCE(total_principal_overdue_condoned, 0) AS total_principal_overdue_condoned,
    COALESCE(total_current_interest_condoned, 0) AS total_current_interest_condoned,
    COALESCE(total_current_principal_condoned, 0) AS total_current_principal_condoned,
    COALESCE(total_unpaid_principal_condoned, 0) AS total_unpaid_principal_condoned,
    COALESCE(total_guarantee_condoned, 0) AS total_guarantee_condoned,
    row_number() OVER (PARTITION BY loan_id, calculation_date ORDER BY calculation_date - condonation_date_original) as priority
  FROM full_base
)
-- STEP 8: Consolidated Datamart
SELECT
    'CO' AS country,
    loan_id,
    condonation_date,
    condonation_reason,
    total_moratory_interest_condoned,
    total_interest_overdue_condoned,
    total_principal_overdue_condoned,
    total_current_interest_condoned,
    total_current_principal_condoned,
    total_unpaid_principal_condoned,
    total_guarantee_condoned,
    total_moratory_interest_condoned + total_interest_overdue_condoned + total_principal_overdue_condoned + total_current_interest_condoned +
    total_current_principal_condoned + total_unpaid_principal_condoned + total_guarantee_condoned AS condoned_amount
    FROM full_base_2
WHERE priority = 1

