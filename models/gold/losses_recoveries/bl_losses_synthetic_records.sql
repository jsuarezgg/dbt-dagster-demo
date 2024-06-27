{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}



WITH loans_to_refinance AS (
	SELECT
		loan_id ,
		EXPLODE(loans_to_refinance_lst_debug.loan_to_refinance_id) AS loan_to_refinance_id
	FROM {{ ref('dm_refinance_loans') }} drl
    -- To handle this particular issue: https://addico.slack.com/archives/C05A3B0KPEZ/p1700772846156439?thread_ts=1700772677.305429&cid=C05A3B0KPEZ
    QUALIFY ROW_NUMBER() OVER (PARTITION BY loan_to_refinance_id ORDER BY origination_date_local) = 1
)
,
loans_refinanced_first_payment_date AS (
	SELECT
		lr.loan_id,
		lr.loan_to_refinance_id,
		ls.first_payment_date AS first_payment_date_original
	FROM loans_to_refinance lr
	LEFT JOIN {{ ref('dm_loan_status_co') }} ls		ON lr.loan_to_refinance_id = ls.loan_id
)
,
ghost_loans AS (
	SELECT
		dls.loan_id,
		CAST(min(from_utc_timestamp(COALESCE(lrfpd.first_payment_date_original, ls.first_payment_date), 'America/Bogota')) AS DATE) AS first_payment_date,
		max(dls.calculation_date) AS max_calculation_date
	FROM {{ source('gold','dm_daily_loan_status_co') }} dls
  LEFT JOIN {{ ref('dm_loan_status_co') }} ls			ON dls.loan_id = ls.loan_id
  LEFT JOIN loans_refinanced_first_payment_date lrfpd	ON dls.loan_id = lrfpd.loan_id
	GROUP BY 1
	HAVING max(dls.calculation_date) < (SELECT max(dls.calculation_date) FROM gold.dm_daily_loan_status_co dls)
)
,
explode_dates AS (
	SELECT
		loan_id,
		first_payment_date AS first_payment_date,
		max_calculation_date AS calculation_date,
		SEQUENCE(first_payment_date::date, first_payment_date::date + INTERVAL 4 YEAR, INTERVAL 1 MONTH) AS next_months,
		ARRAY(
			NAMED_STRUCT('numeric_mob', 0.17, 'next_days', first_payment_date::date + INTERVAL 5 DAY),
			NAMED_STRUCT('numeric_mob',0.33, 'next_days', first_payment_date::date + INTERVAL 10 DAY),
			NAMED_STRUCT('numeric_mob', 0.5, 'next_days', first_payment_date::date + INTERVAL 15 DAY)
		) AS next_days
	FROM ghost_loans
	WHERE loan_id IN (
		SELECT
			loan_id
		FROM ghost_loans
	)
)
,
monthly_view AS (
	SELECT
		loan_id,
		first_payment_date,
		calculation_date ,
		explode(next_months) AS synthetic_calculation_date
	FROM explode_dates
)
,
monthly_months_on_books AS (
		SELECT
		loan_id,
		synthetic_calculation_date + INTERVAL '1 DAY' AS synthetic_calculation_date,
		ROW_NUMBER() OVER(PARTITION BY loan_id ORDER BY synthetic_calculation_date) AS synthetic_mob_number
	FROM monthly_view
	WHERE synthetic_calculation_date <= (SELECT max(calculation_date) FROM gold.dm_daily_loan_status_co)
		AND synthetic_calculation_date > first_payment_date
)
,
daily_view AS (
	SELECT
		loan_id,
		first_payment_date,
		calculation_date ,
		explode(next_days) AS synthetic_calculation_date
	FROM explode_dates
)
,
daily_months_on_books AS (
	SELECT
		loan_id,
		synthetic_calculation_date.next_days AS synthetic_calculation_date,
		synthetic_calculation_date.numeric_mob AS synthetic_mob_number
	FROM daily_view
	WHERE synthetic_calculation_date.next_days <= (SELECT max(calculation_date) FROM gold.dm_daily_loan_status_co)
		AND synthetic_calculation_date.next_days > first_payment_date
)
SELECT * FROM daily_months_on_books
UNION ALL
SELECT * FROM monthly_months_on_books