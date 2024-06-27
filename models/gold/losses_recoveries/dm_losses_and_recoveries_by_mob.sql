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
		EXPLODE(loans_to_refinance_lst_debug.loan_to_refinance_id) AS loan_to_refinance_id,
        CAST(origination_date_local AS DATE) AS refinancing_date
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
loans_base AS (
  SELECT
    ls.loan_id,
    CAST(from_utc_timestamp(COALESCE(lrfpd.first_payment_date_original, ls.first_payment_date), 'America/Bogota') AS DATE) AS first_payment_date,
    ls.approved_amount,
    rmt.condoned_amount_by_loan_recast AS condoned_amount_by_loan_recast,
    CASE WHEN rmt.non_refunded_cancellation IS TRUE THEN ls.approved_amount END AS non_refunded_cancellation_amount,
    rmt.is_fully_paid_v2 AS is_completely_paid,
    rmt.is_fully_paid_date_v2 AS is_completely_paid_date
  FROM {{ ref('dm_loan_status_co') }} ls
  LEFT JOIN {{ ref('risk_master_table_co') }} rmt           ON ls.loan_id = rmt.loan_id
  LEFT JOIN loans_refinanced_first_payment_date lrfpd	    ON ls.loan_id = lrfpd.loan_id
  WHERE ls.loan_id IS NOT NULL
)
,
dm_condonations AS (
  SELECT
    loan_id,
    condonation_date,
    condonation_reason,
    CASE
      WHEN condonation_reason = 'COLLECTIONS' THEN condonation_date
    END AS condonation_date_collections,
    CASE
      WHEN condonation_reason = 'FGA_CLAIM' THEN condonation_date
    END AS condonation_date_fga_claim,
    CASE
      WHEN condonation_reason = 'LOAN_RECAST' THEN condonation_date
    END AS condonation_date_loan_recast,
    CASE
      WHEN condonation_reason = 'OTHER_REASONS' THEN condonation_date
    END AS condonation_date_other_reasons,
    COALESCE(date_add(LEAD(condonation_date) OVER (PARTITION BY loan_id ORDER BY condonation_date), -1), current_date()) AS next_condonation_date,
    COALESCE(
      CASE
        WHEN condonation_reason = 'COLLECTIONS' THEN date_add(LEAD(condonation_date) OVER (PARTITION BY loan_id ORDER BY condonation_date), -1)
      END, current_date()) AS next_condonation_date_collections,
    COALESCE(
      CASE
        WHEN condonation_reason = 'FGA_CLAIM' THEN date_add(LEAD(condonation_date) OVER (PARTITION BY loan_id ORDER BY condonation_date), -1)
      END, current_date()) AS next_condonation_date_fga_claim,
    COALESCE(
      CASE
        WHEN condonation_reason = 'LOAN_RECAST' THEN date_add(LEAD(condonation_date) OVER (PARTITION BY loan_id ORDER BY condonation_date), -1)
      END, current_date()) AS next_condonation_date_loan_recast,
    COALESCE(
      CASE
        WHEN condonation_reason = 'OTHER_REASONS' THEN date_add(LEAD(condonation_date) OVER (PARTITION BY loan_id ORDER BY condonation_date), -1)
      END, current_date()) AS next_condonation_date_other_reasons,
    total_moratory_interest_condoned,
    total_interest_overdue_condoned,
    total_principal_overdue_condoned,
    total_current_interest_condoned,
    total_current_principal_condoned,
    total_unpaid_principal_condoned,
    total_guarantee_condoned,
    condoned_amount
  FROM {{ ref('dm_condonations') }}
  WHERE condonation_date IS NOT NULL
)
,
cum_dm_cond AS (
    SELECT
        loan_id,
        condonation_date,
        condonation_reason,
        condonation_date_collections,
        condonation_date_fga_claim,
        condonation_date_loan_recast,
        condonation_date_other_reasons,
        next_condonation_date,
        next_condonation_date_collections,
        next_condonation_date_fga_claim,
        next_condonation_date_loan_recast,
        next_condonation_date_other_reasons,
        SUM(total_moratory_interest_condoned) OVER (PARTITION BY loan_id, condonation_reason ORDER BY condonation_date RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_moratory_interest_condoned,
        SUM(total_interest_overdue_condoned) OVER (PARTITION BY loan_id, condonation_reason ORDER BY condonation_date RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_interest_overdue_condoned,
        SUM(total_principal_overdue_condoned) OVER (PARTITION BY loan_id, condonation_reason ORDER BY condonation_date RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_principal_overdue_condoned,
        SUM(total_current_interest_condoned) OVER (PARTITION BY loan_id, condonation_reason ORDER BY condonation_date RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_current_interest_condoned,
        SUM(total_current_principal_condoned) OVER (PARTITION BY loan_id, condonation_reason ORDER BY condonation_date RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_current_principal_condoned,
        SUM(total_unpaid_principal_condoned) OVER (PARTITION BY loan_id, condonation_reason ORDER BY condonation_date RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_unpaid_principal_condoned,
        SUM(total_guarantee_condoned) OVER (PARTITION BY loan_id, condonation_reason ORDER BY condonation_date RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_guarantee_condoned,
        SUM(condoned_amount) OVER (PARTITION BY loan_id ORDER BY condonation_date RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS condoned_amount
    FROM dm_condonations
)
,
raw_charge_offs AS (
    SELECT
        loan_id,
        charge_off_date,
        'CREDIT' AS charge_off_reason
    FROM {{ ref('f_snc_charge_off_report_co') }}
    WHERE is_charge_off IS TRUE
    UNION ALL
    SELECT
        loan_id,
        from_utc_timestamp(loan_cancellation_order_date, 'America/Bogota')::date AS charge_off_date,
        'NON_REFUNDED_CANCELLATION' AS charge_off_reason
    FROM {{ ref('f_loan_cancellations_v2_co') }}
    WHERE loan_cancellation_reason IN ('FRAUD','ALLY_BAD_PRACTICE','CLIENT_DEATH')
        AND custom_loan_cancellation_status = 'V2_CANCELLATION_PROCESSED'
        AND loan_id NOT IN (SELECT loan_id FROM {{ ref('f_snc_charge_off_report_co') }} WHERE is_charge_off IS TRUE)
)
,
raw_charge_offs_with_fga AS (
    SELECT
        loan_id,
        min(condonation_date) AS charge_off_date,
        'FGA_CLAIM' AS charge_off_reason
    FROM {{ ref('dm_condonations') }}
    WHERE loan_id NOT IN (SELECT loan_id FROM raw_charge_offs)
        AND condonation_reason = 'FGA_CLAIM'
    GROUP BY 1
    UNION ALL
    SELECT
        loan_id,
        charge_off_date,
        charge_off_reason
    FROM raw_charge_offs
)
,
charge_offs AS (
    SELECT
        cor.loan_id,
        cor.charge_off_date,
        dls.unpaid_principal AS principal_charged_off,
        dls.unpaid_guarantee AS guarantee_charged_off,
        dls.unpaid_interest AS interest_charged_off,
        dls.unpaid_collection_fees AS collection_fees_charged_off
    FROM raw_charge_offs_with_fga cor
    LEFT JOIN {{ source('gold','dm_daily_loan_status_co') }} dls    ON cor.loan_id = dls.loan_id AND cor.charge_off_date = dls.calculation_date
)
,
pmts AS (
    SELECT
        pr.loan_id,
        date_add(pr.`date`, 1) AS payment_date,
        sum(pr.moratory_interest) AS pmt_moratory_interest,
        sum(pr.interest_overdue) AS pmt_interest_overdue,
        sum(pr.principal_overdue) AS pmt_principal_overdue,
        sum(pr.guarantee_overdue) AS pmt_guarantee_overdue,
        sum(pr.current_interest) AS pmt_current_interest,
        sum(pr.current_principal) AS pmt_current_principal,
        sum(pr.current_guarantee) AS pmt_current_guarantee,
        sum(pr.unpaid_guarantee) AS pmt_unpaid_guarantee,
        sum(pr.unpaid_principal) AS pmt_unpaid_principal,
        sum(pr.prepayment_benefit) AS pmt_prepayment_benefit,
        sum(round(pr.collection_fees, 2)) AS pmt_collection_fees,
        min(cor.charge_off_date) AS charge_off_date
    FROM {{ ref('f_snc_payments_report_co') }} pr
    LEFT JOIN charge_offs cor ON pr.loan_id = cor.loan_id
    -- WHERE pr.loan_id = '93e5e717-0c2c-4975-978c-0f15b1b5a083'
    GROUP BY 1,2
),
cum_payments_charge_off AS (
    SELECT
        loan_id,
        payment_date,
        COALESCE(lead(payment_date - INTERVAL '1 day') OVER (PARTITION BY loan_id ORDER BY payment_date), current_date()) AS prev_next_payment_date,
        sum(pmt_moratory_interest) OVER(PARTITION BY loan_id ORDER BY payment_date) AS cum_pmt_moratory_interest,
        sum(pmt_interest_overdue) OVER(PARTITION BY loan_id ORDER BY payment_date) AS cum_pmt_interest_overdue,
        sum(pmt_principal_overdue) OVER(PARTITION BY loan_id ORDER BY payment_date) AS cum_pmt_principal_overdue,
        sum(pmt_guarantee_overdue) OVER(PARTITION BY loan_id ORDER BY payment_date) AS cum_pmt_guarantee_overdue,
        sum(pmt_current_interest) OVER(PARTITION BY loan_id ORDER BY payment_date) AS cum_pmt_current_interest,
        sum(pmt_current_principal) OVER(PARTITION BY loan_id ORDER BY payment_date) AS cum_pmt_current_principal,
        sum(pmt_current_guarantee) OVER(PARTITION BY loan_id ORDER BY payment_date) AS cum_pmt_current_guarantee,
        sum(pmt_unpaid_guarantee) OVER(PARTITION BY loan_id ORDER BY payment_date) AS cum_pmt_unpaid_guarantee,
        sum(pmt_unpaid_principal) OVER(PARTITION BY loan_id ORDER BY payment_date) AS cum_pmt_unpaid_principal,
        sum(pmt_prepayment_benefit) OVER(PARTITION BY loan_id ORDER BY payment_date) AS cum_pmt_prepayment_benefit,
        sum(pmt_collection_fees) OVER(PARTITION BY loan_id ORDER BY payment_date) AS cum_pmt_collection_fees
    FROM pmts
    WHERE payment_date > charge_off_date
)
,
first_dls_record AS (
    SELECT
        loan_id,
        min(calculation_date) AS min_calculation_date,
        min(CASE WHEN bucket = '31 to 60' THEN calculation_date END) AS min_31_plus_dq_date,
        min(CASE WHEN bucket = '61 to 90' THEN calculation_date END) AS min_61_plus_dq_date,
        min(CASE WHEN bucket = '91 to 120' THEN calculation_date END) AS min_91_plus_dq_date
    FROM {{ source('gold','dm_daily_loan_status_co') }} dls
    WHERE NOT EXISTS (
    	SELECT * FROM loans_to_refinance lr WHERE lr.loan_id = dls.loan_id
    )
    GROUP BY 1
)
,
balance_at_default AS (
    SELECT
        dls.loan_id,
        dls.delinquency_balance AS balance_at_default,
        dls.calculation_date AS default_date,
        ROW_NUMBER() OVER(PARTITION BY loan_id ORDER BY calculation_date) AS priority
    FROM {{ source('gold','dm_daily_loan_status_co') }} dls
    WHERE dls.bucket = '120+'
)
,
raw_daily_loan_status AS (
    SELECT
        dls.loan_id,
        dls.calculation_date,
        CASE
            WHEN dls.bucket = 'Current' THEN 0
            WHEN dls.bucket = '1 to 30' THEN 1
            WHEN dls.bucket = '31 to 60' THEN 2
            WHEN dls.bucket = '61 to 90' THEN 3
            WHEN dls.bucket = '91 to 120' THEN 4
            WHEN dls.bucket = '120+' THEN 5
        END AS numeric_bucket,
        lr.loan_to_refinance_id,
        ltr.refinancing_date,
        dls.bucket AS delinquency_bucket,
        dls.days_past_due,
        dls.unpaid_principal AS upb,
        dls.total_principal_paid,
        dls.total_interest_paid,
        dls.total_guarantee_paid,
        dls.total_collection_fees_paid,
        it.approved_amount AS amount,
        it.first_payment_date,
        COALESCE(cum_pmt_principal_overdue + cum_pmt_current_principal + cum_pmt_unpaid_principal, 0.00) AS principal_recovered,
        COALESCE(cum_pmt_moratory_interest + cum_pmt_interest_overdue + cum_pmt_current_interest, 0.00) AS interest_recovered,
        COALESCE(cum_pmt_guarantee_overdue + cum_pmt_current_guarantee + cum_pmt_unpaid_guarantee, 0.00) AS guarantee_recovered,
        COALESCE(cum_pmt_collection_fees, 0.00) AS collection_fees_recovered,
        COALESCE(co.principal_charged_off, 0.00) AS principal_charged_off,
        COALESCE(co.guarantee_charged_off, 0.00) AS guarantee_charged_off,
        COALESCE(co.interest_charged_off, 0.00) AS interest_charged_off,
        COALESCE(co.collection_fees_charged_off, 0.00) AS collection_fees_charged_off,
        COALESCE(dm_cond.total_moratory_interest_condoned + dm_cond.total_current_interest_condoned + dm_cond.total_interest_overdue_condoned, 0.00) AS total_interest_condoned,
        COALESCE(dm_cond.total_principal_overdue_condoned + dm_cond.total_current_principal_condoned + dm_cond.total_unpaid_principal_condoned, 0.00) AS total_principal_condoned,
        COALESCE(dm_cond.total_guarantee_condoned, 0.00) AS total_guarantee_condoned,
        COALESCE(dm_cond_coll.condoned_amount, 0.00) AS condoned_amount_by_collections,
        COALESCE(dm_cond_fga.condoned_amount, 0.00) AS condoned_amount_by_fga_claim,
        COALESCE(dm_cond_rec.condoned_amount, 0.00) AS condoned_amount_by_loan_recast,
        COALESCE(dm_cond_oth.condoned_amount, 0.00) AS condoned_amount_by_other_reasons,
        COALESCE(bad.balance_at_default, 0.00) AS balance_at_default,
        CASE
            WHEN it.non_refunded_cancellation_amount > 0 THEN 0
            WHEN dm_cond_rec.condoned_amount > 0 THEN it.approved_amount - dm_cond_rec.condoned_amount
            ELSE it.approved_amount
        END AS net_opb,
        CASE
            WHEN dls.calculation_date >= it.is_completely_paid_date THEN TRUE
            ELSE FALSE
        END AS is_completely_paid,
        dls.is_fully_paid AS is_fully_settled,
        CASE
            WHEN dls.calculation_date >= dls_min.min_31_plus_dq_date THEN TRUE
            ELSE FALSE
        END AS flag_31_plus_ever,
        CASE
            WHEN dls.calculation_date >= dls_min.min_61_plus_dq_date THEN TRUE
            ELSE FALSE
        END AS flag_61_plus_ever,
        CASE
            WHEN dls.calculation_date >= dls_min.min_91_plus_dq_date THEN TRUE
            ELSE FALSE
        END AS flag_91_plus_ever,
        CASE
            WHEN dls.calculation_date = dls_min.min_calculation_date THEN -1
            WHEN dls.calculation_date = it.first_payment_date THEN 0
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '5 day')::date THEN 0.17
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '10 day')::date THEN 0.33
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '15 day')::date THEN 0.5
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '1 month' + INTERVAL '1 day')::date THEN 1
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '2 month' + INTERVAL '1 day')::date THEN 2
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '3 month' + INTERVAL '1 day')::date THEN 3
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '4 month' + INTERVAL '1 day')::date THEN 4
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '5 month' + INTERVAL '1 day')::date THEN 5
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '6 month' + INTERVAL '1 day')::date THEN 6
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '7 month' + INTERVAL '1 day')::date THEN 7
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '8 month' + INTERVAL '1 day')::date THEN 8
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '9 month' + INTERVAL '1 day')::date THEN 9
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '10 month' + INTERVAL '1 day')::date THEN 10
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '11 month' + INTERVAL '1 day')::date THEN 11
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '12 month' + INTERVAL '1 day')::date THEN 12
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '13 month' + INTERVAL '1 day')::date THEN 13
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '14 month' + INTERVAL '1 day')::date THEN 14
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '15 month' + INTERVAL '1 day')::date THEN 15
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '16 month' + INTERVAL '1 day')::date THEN 16
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '17 month' + INTERVAL '1 day')::date THEN 17
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '18 month' + INTERVAL '1 day')::date THEN 18
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '19 month' + INTERVAL '1 day')::date THEN 19
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '20 month' + INTERVAL '1 day')::date THEN 20
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '21 month' + INTERVAL '1 day')::date THEN 21
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '22 month' + INTERVAL '1 day')::date THEN 22
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '23 month' + INTERVAL '1 day')::date THEN 23
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '24 month' + INTERVAL '1 day')::date THEN 24
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '25 month' + INTERVAL '1 day')::date THEN 25
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '26 month' + INTERVAL '1 day')::date THEN 26
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '27 month' + INTERVAL '1 day')::date THEN 27
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '28 month' + INTERVAL '1 day')::date THEN 28
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '29 month' + INTERVAL '1 day')::date THEN 29
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '30 month' + INTERVAL '1 day')::date THEN 30
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '31 month' + INTERVAL '1 day')::date THEN 31
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '32 month' + INTERVAL '1 day')::date THEN 32
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '33 month' + INTERVAL '1 day')::date THEN 33
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '34 month' + INTERVAL '1 day')::date THEN 34
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '35 month' + INTERVAL '1 day')::date THEN 35
            WHEN dls.calculation_date = (it.first_payment_date + INTERVAL '36 month' + INTERVAL '1 day')::date THEN 36
        END AS mob_number
    FROM {{ source('gold','dm_daily_loan_status_co') }} dls
    LEFT JOIN loans_base it                   ON dls.loan_id = it.loan_id
    LEFT JOIN cum_payments_charge_off cpco    ON dls.loan_id = cpco.loan_id AND dls.calculation_date BETWEEN cpco.payment_date AND cpco.prev_next_payment_date
    LEFT JOIN cum_dm_cond dm_cond             ON dls.loan_id = dm_cond.loan_id AND dls.calculation_date BETWEEN dm_cond.condonation_date AND dm_cond.next_condonation_date
    LEFT JOIN cum_dm_cond dm_cond_coll        ON dls.loan_id = dm_cond_coll.loan_id AND
                                            dls.calculation_date BETWEEN dm_cond_coll.condonation_date_collections AND dm_cond_coll.next_condonation_date_collections
    LEFT JOIN cum_dm_cond dm_cond_fga         ON dls.loan_id = dm_cond_fga.loan_id AND
                                            dls.calculation_date BETWEEN dm_cond_fga.condonation_date_fga_claim AND dm_cond_fga.next_condonation_date_fga_claim
    LEFT JOIN cum_dm_cond dm_cond_rec         ON dls.loan_id = dm_cond_rec.loan_id AND
                                            dls.calculation_date BETWEEN dm_cond_rec.condonation_date_loan_recast AND dm_cond_rec.next_condonation_date_loan_recast
    LEFT JOIN cum_dm_cond dm_cond_oth         ON dls.loan_id = dm_cond_oth.loan_id AND
                                            dls.calculation_date BETWEEN dm_cond_oth.condonation_date_other_reasons AND dm_cond_oth.next_condonation_date_other_reasons
    LEFT JOIN charge_offs co                  ON dls.loan_id = co.loan_id AND dls.calculation_date >= co.charge_off_date
    LEFT JOIN first_dls_record dls_min        ON dls.loan_id = dls_min.loan_id
    LEFT JOIN loans_to_refinance lr		      ON dls.loan_id = lr.loan_id
    LEFT JOIN loans_to_refinance ltr		  ON dls.loan_id = ltr.loan_to_refinance_id
    LEFT JOIN balance_at_default bad          ON dls.loan_id = bad.loan_id AND bad.priority = 1
)
,
latest_records AS (
	SELECT
		*,
	  total_principal_paid - COALESCE(LAG(total_principal_paid) OVER(PARTITION BY loan_id ORDER BY calculation_date), 0.00) AS marginal_principal_paid,
	  total_interest_paid - COALESCE(LAG(total_interest_paid) OVER(PARTITION BY loan_id ORDER BY calculation_date), 0.00) AS marginal_interest_paid,
	  total_guarantee_paid - COALESCE(LAG(total_guarantee_paid) OVER(PARTITION BY loan_id ORDER BY calculation_date), 0.00) AS marginal_guarantee_paid,
	  total_collection_fees_paid - COALESCE(LAG(total_collection_fees_paid) OVER(PARTITION BY loan_id ORDER BY calculation_date), 0.00) AS marginal_collection_fees_paid,
	  GREATEST(net_opb - total_principal_paid, 0) AS OSPB,
	  ROW_NUMBER() OVER(PARTITION BY loan_id ORDER BY calculation_date DESC) AS flag_latest_record
	FROM raw_daily_loan_status
	WHERE mob_number IS NOT NULL
        AND loan_id NOT IN (
            SELECT
                loan_id
            FROM {{ ref('f_loan_cancellations_v2_co') }} flcvc
            WHERE custom_loan_cancellation_status = 'V2_CANCELLATION_PROCESSED'
                AND loan_cancellation_reason NOT IN ('FRAUD','ALLY_BAD_PRACTICE','CLIENT_DEATH')
    )
)
,
syntethic_records AS (
	SELECT
		a.*,
		b.synthetic_calculation_date,
		b.synthetic_mob_number
	FROM latest_records a
	LEFT JOIN {{ ref('bl_losses_synthetic_records') }} b 	ON a.loan_id = b.loan_id
	WHERE a.flag_latest_record = 1
		AND b.synthetic_calculation_date > calculation_date
)
,
unified_losses AS (
	SELECT
		loan_id,
		calculation_date,
		amount,
	    CASE
	  	    WHEN mob_number = 0.17 THEN '5 DAY'
	  	    WHEN mob_number = 0.33 THEN '10 DAY'
	  	    WHEN mob_number = 0.5 THEN '15 DAY'
	  	    ELSE CAST(ROUND(mob_number) AS STRING) || ' MOB'
	    END AS mob,
		mob_number,
		delinquency_bucket,
		numeric_bucket,
		days_past_due,
		upb,
		total_principal_paid,
		total_interest_paid,
		total_guarantee_paid,
		total_collection_fees_paid,
		marginal_principal_paid,
		marginal_interest_paid,
		marginal_guarantee_paid,
		marginal_collection_fees_paid,
		first_payment_date,
		principal_recovered,
		interest_recovered,
		guarantee_recovered,
		collection_fees_recovered,
		principal_charged_off,
		guarantee_charged_off,
		interest_charged_off,
		collection_fees_charged_off,
		balance_at_default,
		total_interest_condoned,
		total_principal_condoned,
		total_guarantee_condoned,
		condoned_amount_by_collections,
		condoned_amount_by_fga_claim,
		condoned_amount_by_loan_recast,
		condoned_amount_by_other_reasons,
		net_opb,
		is_completely_paid,
		is_fully_settled,
		OSPB,
		loan_to_refinance_id,
        refinancing_date
	FROM latest_records
	UNION ALL
	SELECT
		loan_id,
		synthetic_calculation_date AS calculation_date,
		amount,
	    CASE
	        WHEN synthetic_mob_number = 0.17 THEN '5 DAY'
	        WHEN synthetic_mob_number = 0.33 THEN '10 DAY'
	        WHEN synthetic_mob_number = 0.5 THEN '15 DAY'
	        ELSE CAST(ROUND(synthetic_mob_number) AS STRING) || ' MOB'
	    END AS mob,
		synthetic_mob_number AS mob_number,
		delinquency_bucket,
		numeric_bucket,
		days_past_due,
		upb,
		total_principal_paid,
		total_interest_paid,
		total_guarantee_paid,
		total_collection_fees_paid,
		marginal_principal_paid,
		marginal_interest_paid,
		marginal_guarantee_paid,
		marginal_collection_fees_paid,
		first_payment_date,
		principal_recovered,
		interest_recovered,
		guarantee_recovered,
		collection_fees_recovered,
		principal_charged_off,
		guarantee_charged_off,
		interest_charged_off,
		collection_fees_charged_off,
		balance_at_default,
		total_interest_condoned,
		total_principal_condoned,
		total_guarantee_condoned,
		condoned_amount_by_collections,
		condoned_amount_by_fga_claim,
		condoned_amount_by_loan_recast,
		condoned_amount_by_other_reasons,
		net_opb,
		is_completely_paid,
		is_fully_settled,
		OSPB,
		loan_to_refinance_id,
        refinancing_date
	FROM syntethic_records
	WHERE loan_id IN (
	    SELECT
	        loan_id
	    FROM silver.f_loan_cancellations_v2_co flcvc
	    WHERE custom_loan_cancellation_status = 'V2_CANCELLATION_PROCESSED'
	        AND loan_cancellation_reason IN ('FRAUD','ALLY_BAD_PRACTICE','CLIENT_DEATH')
	)
)
,
unified_losses_original_loans AS (
	SELECT
		*
	FROM unified_losses ul
	WHERE loan_to_refinance_id IS NULL
)
,
unified_losses_refinanced_loans AS (
	SELECT
		*
	FROM unified_losses ul
	WHERE loan_to_refinance_id IS NOT NULL
)
,
unified_datamart AS (
	SELECT
	    ol.loan_id,
	    ol.calculation_date,
	    ol.amount,
	    ol.mob,
	    ol.mob_number,
	    COALESCE(ol.delinquency_bucket, rl.delinquency_bucket) AS delinquency_bucket,
	    COALESCE(ol.numeric_bucket, rl.numeric_bucket) AS numeric_bucket,
	    COALESCE(ol.days_past_due, rl.days_past_due) AS days_past_due,
	    ol.upb + COALESCE(rl.upb, 0) AS upb,
	    ol.total_principal_paid + COALESCE(rl.total_principal_paid, 0) AS total_principal_paid,
	    ol.total_interest_paid + COALESCE(rl.total_interest_paid, 0) AS total_interest_paid,
	    ol.total_guarantee_paid + COALESCE(rl.total_guarantee_paid, 0) AS total_guarantee_paid,
	    ol.total_collection_fees_paid + COALESCE(rl.total_collection_fees_paid, 0) AS total_collection_fees_paid,
	    ol.marginal_principal_paid + COALESCE(rl.marginal_principal_paid, 0) AS marginal_principal_paid,
	    ol.marginal_interest_paid + COALESCE(rl.marginal_interest_paid, 0) AS marginal_interest_paid,
	    ol.marginal_guarantee_paid + COALESCE(rl.marginal_guarantee_paid, 0) AS marginal_guarantee_paid,
	    ol.marginal_collection_fees_paid + COALESCE(rl.marginal_collection_fees_paid, 0) AS marginal_collection_fees_paid,
	    ol.first_payment_date,
	    ol.principal_recovered + COALESCE(rl.principal_recovered, 0) AS principal_recovered,
	    ol.interest_recovered + COALESCE(rl.interest_recovered, 0) AS interest_recovered,
	    ol.guarantee_recovered + COALESCE(rl.guarantee_recovered, 0) AS guarantee_recovered,
	    ol.collection_fees_recovered + COALESCE(rl.collection_fees_recovered, 0) AS collection_fees_recovered,
	    COALESCE(rl.principal_charged_off, 0) AS principal_charged_off,
	    COALESCE(rl.guarantee_charged_off, 0) AS guarantee_charged_off,
	    COALESCE(rl.interest_charged_off, 0) AS interest_charged_off,
	    COALESCE(rl.collection_fees_charged_off, 0) AS collection_fees_charged_off,
	    COALESCE(rl.balance_at_default, 0) AS balance_at_default,
	    ol.total_interest_condoned + COALESCE(rl.total_interest_condoned, 0) AS total_interest_condoned,
	    ol.total_principal_condoned + COALESCE(rl.total_principal_condoned, 0) AS total_principal_condoned,
	    ol.total_guarantee_condoned + COALESCE(rl.total_guarantee_condoned, 0) AS total_guarantee_condoned,
	    ol.condoned_amount_by_collections + COALESCE(rl.condoned_amount_by_collections, 0) AS condoned_amount_by_collections,
	    ol.condoned_amount_by_fga_claim + COALESCE(rl.condoned_amount_by_fga_claim, 0) AS condoned_amount_by_fga_claim,
	    ol.condoned_amount_by_loan_recast + COALESCE(rl.condoned_amount_by_loan_recast, 0) AS condoned_amount_by_loan_recast,
	    ol.condoned_amount_by_other_reasons + COALESCE(rl.condoned_amount_by_other_reasons, 0) AS condoned_amount_by_other_reasons,
	    ol.net_opb,
	    COALESCE(rl.is_completely_paid, FALSE) AS is_completely_paid,
	    COALESCE(rl.is_fully_settled, FALSE) AS is_fully_settled,
	    ol.OSPB + COALESCE(rl.OSPB, 0) AS OSPB,
	    rl.loan_id AS refinancing_loan_id,
        ol.refinancing_date
	FROM unified_losses_original_loans ol
	LEFT JOIN unified_losses_refinanced_loans rl 	ON ol.loan_id = rl.loan_to_refinance_id AND ol.calculation_date = rl.calculation_date
)
,
numeric_bucket_mob_based AS (
	SELECT
		*,
		CASE
			WHEN days_past_due > LAG(days_past_due) OVER(PARTITION BY loan_id ORDER BY mob_number)
				AND numeric_bucket = LAG(numeric_bucket) OVER(PARTITION BY loan_id ORDER BY mob_number)
				AND numeric_bucket BETWEEN 1 AND 4
				AND mob_number >= 1
				THEN LAG(numeric_bucket) OVER(PARTITION BY loan_id ORDER BY mob) + 1
			ELSE numeric_bucket
			END AS numeric_bucket_mob_based
	FROM unified_datamart
)
SELECT
	*,
	CASE
		WHEN numeric_bucket_mob_based = 0 THEN 'Current'
		WHEN numeric_bucket_mob_based = 1 THEN '1 to 30'
		WHEN numeric_bucket_mob_based = 2 THEN '31 to 60'
		WHEN numeric_bucket_mob_based = 3 THEN '61 to 90'
		WHEN numeric_bucket_mob_based = 4 THEN '91 to 120'
		WHEN numeric_bucket_mob_based = 5 THEN '120+'
	END AS delinquency_bucket_mob_based
FROM numeric_bucket_mob_based