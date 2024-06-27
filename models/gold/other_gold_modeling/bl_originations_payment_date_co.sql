{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH m_w_payment_dates AS (
  SELECT DISTINCT
         'MONTHLY' AS reportTerm,
         MIN(day_) OVER (PARTITION BY date_trunc('month',day_) ORDER BY day_ ) AS paymentDate,
         date_trunc('month',MIN(day_) OVER (PARTITION BY date_trunc('month',day_) ORDER BY day_ )) AS paymentDate_trunced
  FROM {{ source('gold', 'dm_support_daily_calendar_series') }}
  WHERE 1=1
        AND dow_iso = 3 --keep only wednesday (payment day)
        AND year(day_) between 2022 AND 2025
        AND dayofmonth(day_) >= 3 --As a business rule, payments are made the first wed of each month, unless Wed is the 1st or 2nd day because report is made the first monday of each month

  UNION ALL

  SELECT DISTINCT
        'WEEKLY' AS reportTerm,
        day_ AS paymentDate,
        date_trunc('week',day_) AS paymentDate_trunced
  FROM {{ source('gold', 'dm_support_daily_calendar_series') }}
  WHERE 1=1
    AND dow_iso = 3 --keep only wednesday (payment day)
    AND year(day_) between 2022 AND 2025
)
, ally_config AS (
  SELECT
        ally_slug,
        data:productPolicies[0].paymentConfig.paymentTerm AS reportTerm,
        data:productPolicies[0].paymentConfig.paymentDistance AS paymentTerm,
        start_date_validity,
        end_date_validity,
        updated_at,
        ROW_NUMBER() OVER (PARTITION BY ally_slug ORDER BY start_date_validity DESC, end_date_validity DESC) AS ordered
  FROM {{ ref('d_ally_management_ally_config_co') }} 
  WHERE 1=1
        AND data:productPolicies[0].paymentConfig.paymentTerm is not null
        AND data:productPolicies[0].paymentConfig.paymentDistance IS NOT NULL
  QUALIFY ordered = 1
) 
, originations AS (
  SELECT
        ob.loan_id,
        ob.application_id,
        ob.ally_slug,
        s.payment_id,
        from_utc_timestamp(ob.origination_date,"America/Bogota") AS origination_date,
        COALESCE(p.payment_date, p.scheduled_payment_date,mp.paymentDate, wp.paymentDate,
        --the report in the daily is generated at midnight, so the payment is one day more than the paymentTerm
        CASE WHEN COALESCE(p.report_term, ac.reportTerm) = 'DAILY' THEN date_add(s.occurred_on, CAST(COALESCE(p.payment_term, ac.paymentTerm) AS INTEGER)+1) 
        END) AS payment_date,
        COALESCE(p.report_term, ac.reportTerm) AS report_term,
        COALESCE(p.payment_term, ac.paymentTerm) AS payment_term
  FROM {{ ref('f_originations_bnpl_co') }} ob
  LEFT JOIN {{ ref('f_ally_management_ally_payment_transaction_scheduled_payments_co') }} s ON  s.loan_id = ob.loan_id 
                                                                                                AND s.type = 'PURCHASE'
                                                                                                AND ((s.status = 'ASSIGNED' AND s.payment_id IS NOT NULL)
                                                                                                OR (s.status = 'PENDING' AND payment_id IS  NULL))
                                                                                                AND ob.ally_slug != 'addi-marketplace'
  LEFT JOIN {{ ref('f_ally_management_ally_payments_co') }} p ON s.payment_id = p.id
  LEFT JOIN ally_config ac ON ob.ally_slug = ac.ally_slug
  LEFT JOIN m_w_payment_dates mp ON COALESCE(p.report_term, ac.reportTerm) = mp.reportTerm
                                    AND date_trunc('month',add_months(from_utc_timestamp(ob.origination_date,"America/Bogota"),1)) = mp.paymentDate_trunced
                                    AND mp.reportTerm = 'MONTHLY'
  LEFT JOIN m_w_payment_dates wp ON COALESCE(p.report_term, ac.reportTerm) = wp.reportTerm
                                    AND date_trunc('week',date_add(from_utc_timestamp(ob.origination_date,"America/Bogota"),7)) = wp.paymentDate_trunced
                                    AND wp.reportTerm = 'WEEKLY'
) 
SELECT 
    loan_id,  
    report_term, 
    --Payments that are Monthly or Weekly do not have a payment_term, the logic is the first working week, etc.
    CASE WHEN report_term = 'DAILY' THEN payment_term ELSE null END AS payment_term, 
    payment_date,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
   
FROM originations